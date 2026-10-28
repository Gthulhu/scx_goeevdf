package main

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"runtime"
	"sync"
	"sync/atomic"
	"syscall"
	"time"
	"unsafe"

	"github.com/Gthulhu/scx_goeevdf/eevdf"
	"github.com/Gthulhu/scx_goeevdf/internal/bpf"
	"github.com/Gthulhu/scx_goeevdf/internal/shard"
)

// shardBackend only writes commands into its own shard's command ring.
type shardBackend struct {
	backend
	id int
}

func (b shardBackend) Submit(c eevdf.Cmd) error {
	return b.sched.SubmitShard(b.id, &bpf.Cmd{
		Kind: c.Kind, Pid: c.Pid, Cpu: c.Cpu, Cflags: c.Cflags,
		Slice: c.Slice, EnqFlags: c.EnqFlags, EnqSeq: c.EnqSeq,
		Slot: c.Slot, ExpectCurr: c.ExpectCurr, KickFlags: c.KickFlags,
	})
}

func shardCPUs(groups [][]int32, possible int) ([]uint32, error) {
	if possible <= 0 || possible > bpf.MaxCPUs || len(groups) == 0 || len(groups) > bpf.MaxShards {
		return nil, fmt.Errorf("invalid shard topology: %d CPUs, %d shards", possible, len(groups))
	}
	owners := make([]uint32, possible)
	seen := make([]bool, possible)
	for id, cpus := range groups {
		if len(cpus) == 0 {
			return nil, fmt.Errorf("empty shard %d", id)
		}
		for _, cpu := range cpus {
			if cpu < 0 || int(cpu) >= possible || seen[cpu] {
				return nil, fmt.Errorf("invalid or duplicate CPU %d", cpu)
			}
			owners[cpu], seen[cpu] = uint32(id), true
		}
	}
	for cpu, ok := range seen {
		if !ok {
			return nil, fmt.Errorf("missing CPU %d", cpu)
		}
	}
	return owners, nil
}

func localTopology(t eevdf.Topology, cpus []int32) (eevdf.Topology, error) {
	out := eevdf.Topology{
		Core: make([]int, len(cpus)), LLC: make([]int, len(cpus)),
		Node: make([]int, len(cpus)), Capacity: make([]uint64, len(cpus)),
	}
	for local, cpu := range cpus {
		if cpu < 0 || int(cpu) >= len(t.Core) || int(cpu) >= len(t.LLC) ||
			int(cpu) >= len(t.Node) || int(cpu) >= len(t.Capacity) {
			return eevdf.Topology{}, fmt.Errorf("missing topology for CPU %d", cpu)
		}
		out.Core[local], out.LLC[local] = t.Core[cpu], t.LLC[cpu]
		out.Node[local], out.Capacity[local] = t.Node[cpu], t.Capacity[cpu]
	}
	return out, nil
}

func shardEvent(ev bpf.Ev) eevdf.Event {
	return eevdf.Event{
		Kind: ev.Kind, Pid: ev.Pid, Tgid: ev.Tgid, Cpu: ev.Cpu,
		Flags: ev.Flags, Now: ev.Now, SumExec: ev.SumExec,
		Weight: ev.Weight, CustomSlice: ev.CustomSlice,
		WakerPid: ev.WakerPid, WakerCpu: ev.WakerCpu,
		WakeFlags: ev.WakeFlags, Policy: ev.Policy, EnqSeq: ev.EnqSeq,
		Cgid: ev.Cgid,
	}
}

// managedShard is called solely from its coordinator owner's Step.
type managedShard struct {
	*shard.PolicyEngine
	sched          *bpf.Sched
	pressure       *eevdf.Pressure
	governor       *eevdf.FrequencyGovernor
	groups         groupSync
	cpus           []int32
	samples        []eevdf.StatSample
	id             int
	nextTick       uint64
	nextStat       uint64
	drops          uint64
	recoverTo      uint64
	nextQuotaCheck uint64
	newWork        map[int32]bool
	multi          bool
	quotaPool      *eevdf.SharedQuotaPool
	quotaSnapshot  *shardQuotaSnapshot
}

type shardQuota struct {
	quota, period uint64
}

type shardQuotaSnapshot struct {
	paths  []string
	groups map[uint64]shardQuota
}

func (s *shardQuotaSnapshot) permits(id, quota, period uint64) bool {
	if expected, known := s.groups[id]; known {
		return expected == (shardQuota{quota, period})
	}
	return quota == 0
}

func (s *shardQuotaSnapshot) require(id, quota, period uint64, path string) error {
	if !s.permits(id, quota, period) {
		return fmt.Errorf("dynamic cpu.max at %s is unsupported in sharded mode; restart or use --shards 1", path)
	}
	return nil
}

// Dynamic cpu.max changes are not supported in sharded mode. Configuration
// broadcasts only enqueue mailbox work: while one owner installs a new finite
// limit, another can still dispatch without its local bwGroup. Nor does a
// cgroup event order the write against already queued events on other rings.
// Stop rather than silently run with a partially installed quota.
func (m *managedShard) checkQuota(path string) error {
	if !m.multi {
		return nil
	}
	for path != "" {
		quota, period, err := readQuota(path)
		if err != nil {
			if path != "/sys/fs/cgroup" && errors.Is(err, os.ErrNotExist) {
				return nil // the task's cgroup was removed during lookup
			}
			return fmt.Errorf("cannot verify cpu.max at %s: %w", path, err)
		}
		id, err := cgroupID(path)
		if err != nil {
			if path != "/sys/fs/cgroup" && errors.Is(err, os.ErrNotExist) {
				return nil
			}
			return err
		}
		if err := m.quotaSnapshot.require(id, quota, period, path); err != nil {
			return err
		}
		if path == "/sys/fs/cgroup" {
			break
		}
		path = filepath.Dir(path)
	}
	return nil
}

func (m *managedShard) configureGroup(path string, id uint64) error {
	got, err := cgroupID(path)
	if err != nil || got != id {
		return nil // the task exited or changed cgroups before this event
	}
	if err := m.checkQuota(path); err != nil {
		return err
	}
	if got, err := cgroupID(path); err != nil || got != id {
		return nil
	}
	if err := m.groups.configure(m.Shard.Policy, path); err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil
		}
		return err
	}
	if m.groups.last == nil {
		m.groups.last = make(map[uint64]time.Time)
	}
	m.groups.last[id] = time.Now()
	return nil
}

func (m *managedShard) syncGroup(pid int32, id uint64) error {
	if id == 0 {
		return nil
	}
	path, err := cgroupPath(pid)
	if err != nil {
		return nil // short-lived task exited before its enqueue was processed
	}
	return m.configureGroup(path, id)
}

func (m *managedShard) Handle(slot int32, ev eevdf.Event) error {
	if ev.Kind == eevdf.EvEnqueue {
		if err := m.syncGroup(ev.Pid, ev.Cgid); err != nil {
			return err
		}
	}
	if err := m.PolicyEngine.Handle(slot, ev); err != nil {
		return err
	}
	if ev.Kind == eevdf.EvEnqueue {
		if local, ok := m.Shard.Policy.TaskCPU(ev.Pid); ok {
			if m.newWork == nil {
				m.newWork = make(map[int32]bool)
			}
			m.newWork[m.cpus[local]] = true
		}
	}
	return nil
}

func (m *managedShard) syncImportGroup(pid int32) error {
	path, err := cgroupPath(pid)
	if err != nil {
		return nil
	}
	id, err := cgroupID(path)
	if err != nil {
		return nil
	}
	return m.configureGroup(path, id)
}

func (m *managedShard) Import(pid, slot, cpu int32, image shard.Image) error {
	if err := m.syncImportGroup(pid); err != nil {
		return err
	}
	if err := m.PolicyEngine.Import(pid, slot, cpu, image); err != nil {
		return err
	}
	if m.newWork == nil {
		m.newWork = make(map[int32]bool)
	}
	m.newWork[cpu] = true
	return nil
}

func (m *managedShard) Configure(cfg shard.Config) error {
	if err := m.PolicyEngine.Configure(cfg); err != nil {
		return err
	}
	switch cfg.Kind {
	case shard.ConfigGroup:
		if m.groups.applied == nil {
			m.groups.applied = make(map[uint64]groupConfig)
		}
		config := m.groups.applied[cfg.ID]
		config.parent, config.weight, config.idle = cfg.Parent, cfg.Weight, cfg.Idle
		m.groups.applied[cfg.ID] = config
	case shard.ConfigQuota:
		if m.groups.applied == nil {
			m.groups.applied = make(map[uint64]groupConfig)
		}
		config := m.groups.applied[cfg.ID]
		config.quota, config.period = cfg.Quota, cfg.Period
		m.groups.applied[cfg.ID] = config
	case shard.ConfigRemoveGroup:
		delete(m.groups.last, cfg.ID)
		delete(m.groups.applied, cfg.ID)
		delete(m.groups.paths, cfg.ID)
	}
	return nil
}

func (m *managedShard) Tick() error {
	p := m.Shard.Policy
	now := clockNow()
	if now >= m.nextQuotaCheck {
		if err := m.checkQuota("/sys/fs/cgroup"); err != nil {
			return err
		}
		for _, path := range m.groups.paths {
			if _, err := cgroupID(path); err != nil {
				continue // cgroup was removed before its exit event arrived
			}
			if err := m.checkQuota(path); err != nil {
				return err
			}
		}
		m.nextQuotaCheck = now + 1_000_000_000
	}
	if now >= m.nextTick {
		if m.id == 0 && m.quotaPool != nil {
			m.quotaPool.Refill(now)
		}
		p.Periodic(now)
		p.BalanceTick(now)
		m.nextTick = now + 1_000_000
		if drops := m.sched.Counter("nr_ev_drops"); drops != m.drops {
			m.drops, m.recoverTo = drops, now+1_000_000_000
		}
		if now < m.recoverTo {
			p.ReconcileDropped()
		}
		if now >= m.nextStat {
			if data, err := os.ReadFile("/proc/stat"); err == nil {
				if eevdf.ParseProcStat(data, m.samples, 100) == nil {
					for _, cpu := range m.cpus {
						demand := atomic.LoadUint32(&m.sched.CPU[cpu].CurrPid) != 0 ||
							atomic.LoadUint64(&m.sched.Ctl[cpu].NrQueued) != 0 ||
							atomic.LoadUint64(&m.sched.CPU[cpu].ReleasedAt) != 0
						m.pressure.Update(cpu, now, demand,
							atomic.LoadUint64(&m.sched.CPU[cpu].DisplacedNs), m.samples[cpu])
					}
				}
			}
			m.nextStat = now + 32_000_000
		}
	}
	if err := m.PolicyEngine.Tick(); err != nil {
		return err
	}
	if p.F.Cpufreq {
		for local, cpu := range m.cpus {
			util, capacity, runnable := p.FrequencySample(int32(local), now)
			if target, changed := m.governor.Update(cpu, now, util, capacity, runnable, m.newWork[cpu]); changed {
				m.sched.SetPerf(cpu, target)
			}
		}
	}
	clear(m.newWork)
	if p.Pending() {
		m.sched.SetNrScheduledShard(m.id, 1)
	} else {
		m.sched.SetNrScheduledShard(m.id, 0)
	}
	return nil
}

func bindShardCPU(cpu int32) error {
	if cpu < 0 || cpu >= bpf.MaxCPUs {
		return syscall.EINVAL
	}
	var mask [bpf.MaxCPUs / 8]byte
	mask[cpu/8] = 1 << (uint(cpu) % 8)
	_, _, errno := syscall.RawSyscall(syscall.SYS_SCHED_SETAFFINITY,
		0, uintptr(len(mask)), uintptr(unsafe.Pointer(&mask[0])))
	if errno != 0 {
		return errno
	}
	return nil
}

func workerCPU(cpus []int32, allowed []int32) (int32, error) {
	for _, cpu := range cpus {
		for _, permitted := range allowed {
			if cpu == permitted {
				return cpu, nil
			}
		}
	}
	return 0, fmt.Errorf("no permitted scheduler CPU in shard %v", cpus)
}

// Snapshot cgroup IDs and quotas before binding any policy or attaching BPF.
// Directory order puts parents before children for groupSync.configure.
func readShardQuotaSnapshot(root string) (*shardQuotaSnapshot, error) {
	snapshot := &shardQuotaSnapshot{groups: make(map[uint64]shardQuota)}
	err := filepath.WalkDir(root, func(path string, entry fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if !entry.IsDir() {
			return nil
		}
		quota, period, err := readQuota(path)
		if err != nil {
			return err
		}
		id, err := cgroupID(path)
		if err != nil {
			return err
		}
		snapshot.groups[id] = shardQuota{quota, period}
		snapshot.paths = append(snapshot.paths, path)
		return nil
	})
	return snapshot, err
}

// groupBroadcaster orders cgroup configuration before Route across all ring
// readers. The coordinator enqueues each update into every owner mailbox; it
// never calls a Policy on a ring reader's goroutine.
type groupBroadcaster struct {
	mu       sync.Mutex
	paths    map[uint64]string
	applied  map[uint64]groupConfig
	snapshot *shardQuotaSnapshot
}

func newGroupBroadcaster(snapshot *shardQuotaSnapshot, owners []*managedShard) (*groupBroadcaster, error) {
	b := &groupBroadcaster{
		paths: make(map[uint64]string), applied: make(map[uint64]groupConfig),
		snapshot: snapshot,
	}
	if snapshot == nil {
		return b, nil
	}
	for _, path := range snapshot.paths {
		id, err := cgroupID(path)
		if err != nil {
			return nil, err
		}
		config, ok := owners[0].groups.applied[id]
		if !ok {
			return nil, fmt.Errorf("cgroup %d was not configured before attach", id)
		}
		if expected := snapshot.groups[id]; config.quota != expected.quota || config.period != expected.period {
			return nil, fmt.Errorf("cgroup %d cpu.max changed during shard startup", id)
		}
		for shardID := 1; shardID < len(owners); shardID++ {
			if owners[shardID].groups.applied[id] != config {
				return nil, fmt.Errorf("cgroup %d configuration differs on shard %d", id, shardID)
			}
		}
		b.paths[id] = path
		b.applied[id] = config
	}
	return b, nil
}

func (b *groupBroadcaster) announcePath(c *shard.Coordinator, path string) error {
	if path != "/sys/fs/cgroup" {
		if err := b.announcePath(c, filepath.Dir(path)); err != nil {
			return err
		}
	}
	id, err := cgroupID(path)
	if err != nil {
		return err
	}
	quota, period, err := readQuota(path)
	if err != nil {
		return err
	}
	if b.snapshot != nil {
		if err := b.snapshot.require(id, quota, period, path); err != nil {
			return err
		}
	}
	parent := uint64(0)
	if path != "/sys/fs/cgroup" {
		if parent, err = cgroupID(filepath.Dir(path)); err != nil {
			return err
		}
	}
	idle, err := readIdle(path)
	if err != nil {
		return err
	}
	config := groupConfig{parent: parent, weight: readWeight(path), quota: quota, period: period,
		idle: path != "/sys/fs/cgroup" && idle}
	previous, known := b.applied[id]
	if known && previous == config {
		return nil
	}
	if err := c.BroadcastConfig(shard.Config{
		Kind: shard.ConfigGroup, ID: id, Parent: parent, Weight: config.weight,
		Idle: config.idle, Root: path == "/sys/fs/cgroup",
	}); err != nil {
		return err
	}
	if !known || previous.quota != quota || previous.period != period {
		if err := c.BroadcastConfig(shard.Config{
			Kind: shard.ConfigQuota, ID: id, Quota: quota, Period: period,
		}); err != nil {
			return err
		}
	}
	b.paths[id], b.applied[id] = path, config
	return nil
}

func (b *groupBroadcaster) beforeEnqueue(c *shard.Coordinator, pid int32, id uint64) error {
	if id == 0 {
		return nil
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	if path := b.paths[id]; path != "" {
		return nil // the owner still verifies the live cpu.max before handling
	}
	path, err := cgroupPath(pid)
	if err != nil {
		return nil // task may have exited before the ring was drained
	}
	got, err := cgroupID(path)
	if err != nil || got != id {
		return nil // cgroup move or removal raced with the enqueue event
	}
	if err := b.announcePath(c, path); err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil
		}
		return err
	}
	return nil
}

func (b *groupBroadcaster) event(c *shard.Coordinator, kind uint32, id uint64) error {
	if id == 0 {
		return nil
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	if kind == bpf.EvCgrpExit {
		if err := c.BroadcastConfig(shard.Config{Kind: shard.ConfigRemoveGroup, ID: id}); err != nil {
			return err
		}
		delete(b.paths, id)
		delete(b.applied, id)
		return nil
	}
	path := b.paths[id]
	if path == "" {
		return nil // no owner has seen this cgroup yet
	}
	if actual, err := cgroupID(path); err != nil || actual != id {
		return nil // deletion/reuse is resolved by the exit event
	}
	if err := b.announcePath(c, path); err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil
		}
		return err
	}
	return nil
}

// Request a bounded remote pull when a local CPU is idle and another shard
// has runnable work. Return the source owner so the caller can wake its BPF
// scheduler thread after enqueuing the mailbox request.
func pullRemote(c *shard.Coordinator, topo eevdf.Topology, id int, idle func(int32) bool) (int, error) {
	if c.Shards() < 2 {
		return -1, nil
	}
	for _, dst := range c.CPUs(id) {
		local, ok := c.Snapshot(dst)
		if !ok || local.Queued != 0 || !idle(dst) {
			continue
		}
		var source int32 = -1
		sourceOwner := -1
		var most uint64
		for src := range topo.Core {
			cpu := int32(src)
			owner, ok := c.OwnerCPU(cpu)
			if !ok || owner == id {
				continue
			}
			if snap, ok := c.Snapshot(cpu); ok && snap.Queued > most {
				source, most = cpu, snap.Queued
				sourceOwner = owner
			}
		}
		if source < 0 || most == 0 {
			continue
		}
		level := 2 // system
		if topo.Node[source] == topo.Node[dst] {
			level = 1 // node
		}
		if err := c.Steal(source, dst, level); err != nil {
			return -1, err
		}
		return sourceOwner, nil
	}
	return -1, nil
}

func shardWorker(ctx context.Context, s *bpf.Sched, c *shard.Coordinator, broadcaster *groupBroadcaster, id int,
	cpu int32, topo eevdf.Topology, ready chan<- error, start <-chan struct{}) error {
	runtime.LockOSThread()
	defer runtime.UnlockOSThread()
	if err := bindShardCPU(cpu); err != nil {
		ready <- fmt.Errorf("shard %d affinity: %w", id, err)
		return nil
	}
	tid, _, errno := syscall.RawSyscall(syscall.SYS_GETTID, 0, 0, 0)
	if errno != 0 {
		ready <- errno
		return nil
	}
	if err := s.RegisterUsersched(uint32(tid), uint32(id)); err != nil {
		ready <- fmt.Errorf("register shard %d thread: %w", id, err)
		return nil
	}
	ready <- nil
	select {
	case <-start:
	case <-ctx.Done():
		return nil
	}
	nextPull := uint64(0)
	var gc taskGC
	for ctx.Err() == nil {
		count := 0
		for ; count < 512; count++ {
			var ev bpf.Ev
			ok, err := s.ConsumeShard(id, &ev)
			if err != nil {
				return fmt.Errorf("shard %d event: %w", id, err)
			}
			if !ok {
				break
			}
			gc.observe(ev)
			if ev.Kind == bpf.EvCgrpExit || ev.Kind == bpf.EvCgrpWeight ||
				ev.Kind == bpf.EvCgrpIdle || ev.Kind == bpf.EvCgrpBW {
				if err := broadcaster.event(c, ev.Kind, ev.Cgid); err != nil {
					return fmt.Errorf("shard %d cgroup %d: %w", id, ev.Cgid, err)
				}
				continue
			}
			if ev.Kind >= bpf.EvCgrpInit && ev.Kind <= bpf.EvCgrpBW {
				continue
			}
			if ev.Kind == bpf.EvEnqueue {
				s.DecNrQueuedShard(id, 1)
				if err := broadcaster.beforeEnqueue(c, ev.Pid, ev.Cgid); err != nil {
					return fmt.Errorf("shard %d enqueue cgroup: %w", id, err)
				}
			}
			if ev.Slot == 0 && ev.Kind != bpf.EvEnqueue && ev.Kind != bpf.EvIdle {
				if _, known := c.Slot(ev.Pid); !known {
					continue // an unregistered task exited before first enqueue
				}
			}
			if err := c.Route(int32(ev.Slot), shardEvent(ev)); err != nil {
				return fmt.Errorf("shard %d route slot %d: %w", id, ev.Slot, err)
			}
			if slot, registered := c.Slot(ev.Pid); registered {
				if owner, ok := c.Owner(ev.Pid, slot); ok && owner != id {
					s.SetNeededShard(owner)
					if err := s.KickIdle(c.CPUs(owner)[0]); err != nil {
						return fmt.Errorf("wake shard %d: %w", owner, err)
					}
				}
			}
		}
		stepped, err := c.Step(id)
		if err != nil {
			return fmt.Errorf("shard %d step: %w", id, err)
		}
		if (count != 0 || stepped != 0) && c.Shards() > 1 {
			// A step can enqueue a handoff into another owner's mailbox.
			for other := 0; other < c.Shards(); other++ {
				if other != id {
					s.SetNeededShard(other)
					if err := s.KickIdle(c.CPUs(other)[0]); err != nil {
						return fmt.Errorf("wake shard %d: %w", other, err)
					}
				}
			}
		}
		if now := clockNow(); now >= nextPull {
			if kind := s.Counter("exit_kind"); kind != 0 {
				return fmt.Errorf("kernel scheduler exited: kind=%d code=%d", kind, int64(s.Counter("exit_code")))
			}
			if err := gc.collect(now, s.Counter("nr_ev_drops"),
				func(pid int32) error { return syscall.Kill(int(pid), 0) },
				func(ev bpf.Ev) error {
					slot, ok := c.Slot(ev.Pid)
					if !ok {
						return nil
					}
					return c.Route(slot, shardEvent(ev))
				}); err != nil {
				return err
			}
			source, err := pullRemote(c, topo, id, func(cpu int32) bool {
				return atomic.LoadUint32(&s.CPU[cpu].Idle) != 0
			})
			if err != nil {
				return fmt.Errorf("shard %d remote pull: %w", id, err)
			}
			if source >= 0 {
				s.SetNeededShard(source)
				if err := s.KickIdle(c.CPUs(source)[0]); err != nil {
					return fmt.Errorf("wake shard %d: %w", source, err)
				}
			}
			nextPull = now + 10_000_000
		}
		if count == 0 {
			time.Sleep(100 * time.Microsecond)
		}
	}
	return nil
}

// runSharded runs one pinned, registered scheduling thread and one BPF ring
// consumer per LLC shard. The caller supplies sysfs-derived topology/groups.
// Startup cpu.max budgets are shared across owners. Runtime quota changes
// still stop rather than applying a partially installed limit.
func runSharded(ctx context.Context, object string, features eevdf.Features,
	topo eevdf.Topology, groups [][]int32) error {
	enums, features, err := runtimeEnums(features)
	if err != nil {
		return err
	}
	ncpu, err := possibleCPUs()
	if err != nil {
		return err
	}
	if len(groups) == 0 {
		groups = llcGroups(topo)
	}
	t, err := shard.NewTopology(ncpu, groups, false)
	if err != nil {
		return err
	}
	groups = t.CPUs
	owners, err := shardCPUs(groups, ncpu)
	if err != nil {
		return err
	}
	var quotaSnapshot *shardQuotaSnapshot
	var quotaPool *eevdf.SharedQuotaPool
	if len(groups) > 1 {
		quotaSnapshot, err = readShardQuotaSnapshot("/sys/fs/cgroup")
		if err != nil {
			return run(ctx, object, features)
		}
		quotaPool = eevdf.NewSharedQuotaPool()
		for id, config := range quotaSnapshot.groups {
			if err := quotaPool.Configure(id, config.quota, config.period); err != nil {
				return err
			}
		}
	}
	allowed, err := (backend{}).Affinity(0)
	if err != nil {
		return err
	}
	pinned := make([]int32, len(groups))
	for id, cpus := range groups {
		if pinned[id], err = workerCPU(cpus, allowed); err != nil {
			return err
		}
	}
	s, err := bpf.OpenPath(object)
	if err != nil {
		return err
	}
	defer s.Close()
	s.SetKickFlags(enums.KickIdle, enums.KickPreempt)
	if err := s.SetUserschedTgid(uint32(os.Getpid())); err != nil {
		return err
	}
	if err := s.SetShards(owners, uint32(len(groups))); err != nil {
		return err
	}
	heartbeatCPUs := make([]uint32, len(pinned))
	for id, cpu := range pinned {
		heartbeatCPUs[id] = uint32(cpu)
	}
	if err := s.SetUserschedCPUs(heartbeatCPUs); err != nil {
		return err
	}
	if err := s.Load(); err != nil {
		return err
	}
	engines := make([]shard.Engine, len(groups))
	managed := make([]*managedShard, len(groups))
	for id, cpus := range groups {
		local, err := localTopology(topo, cpus)
		if err != nil {
			return err
		}
		pressure := eevdf.NewPressure(ncpu)
		io := shardBackend{backend{sched: s, pressure: pressure}, id}
		var adapter *shard.PolicyEngine
		if quotaPool != nil {
			adapter, err = shard.NewPolicyEngineWithQuota(io, cpus, policyEnums(enums), quotaPool)
		} else {
			adapter, err = shard.NewPolicyEngine(io, cpus, policyEnums(enums))
		}
		if err != nil {
			return err
		}
		adapter.Shard.Policy.F = features
		adapter.Shard.Policy.F.Normalize()
		if err := adapter.Shard.Policy.SetTopology(local); err != nil {
			return err
		}
		managed[id] = &managedShard{
			PolicyEngine: adapter, sched: s, pressure: pressure,
			governor: eevdf.NewFrequencyGovernor(ncpu, 10_000_000),
			cpus:     cpus, samples: make([]eevdf.StatSample, ncpu), id: id,
			multi: len(groups) > 1, quotaPool: quotaPool, quotaSnapshot: quotaSnapshot,
		}
		if quotaSnapshot != nil {
			for _, path := range quotaSnapshot.paths {
				if err := managed[id].groups.configure(adapter.Shard.Policy, path); err != nil {
					return fmt.Errorf("shard %d cgroup %s: %w", id, path, err)
				}
			}
		}
		engines[id] = managed[id]
	}
	c, err := shard.New(t, engines)
	if err != nil {
		return err
	}
	broadcaster, err := newGroupBroadcaster(quotaSnapshot, managed)
	if err != nil {
		return err
	}
	runCtx, cancel := context.WithCancel(ctx)
	ready := make(chan error, len(groups))
	start := make(chan struct{})
	results := make(chan error, len(groups))
	var wg sync.WaitGroup
	for id := range groups {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			results <- shardWorker(runCtx, s, c, broadcaster, id, pinned[id], topo, ready, start)
		}(id)
	}
	defer func() {
		cancel()
		wg.Wait() // Close must never race a ring reader or command writer.
	}()
	for range groups {
		if err := <-ready; err != nil {
			return err
		}
	}
	if err := s.Attach(); err != nil {
		return err
	}
	close(start)
	select {
	case err := <-results:
		if err != nil {
			return err
		}
		if ctx.Err() == nil {
			return errors.New("shard worker exited unexpectedly")
		}
		return nil
	case <-ctx.Done():
		return nil
	}
}
