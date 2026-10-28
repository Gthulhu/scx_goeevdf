package eevdf

import (
	"bytes"
	"encoding/gob"
	"errors"
	"fmt"
	"sync"
)

// ShardPolicy owns one Policy with dense local CPU indices. Only its owning
// goroutine may call its methods; a coordinator may serialize those calls.
// The IO receives physical CPU IDs, whereas Policy internals see local IDs.
type ShardPolicy struct {
	Policy      *Policy
	global      []int32
	local       map[int32]int32
	io          IO
	suppressPID int32
	stops       map[int32]shardStop
}

type shardStop struct {
	slot, dst int32
	seq       uint64
}

// ErrTaskNotTransferable indicates a normal ownership state that must remain
// on its source until a kernel event makes it pending. It is not corruption.
var ErrTaskNotTransferable = errors.New("task is not safely transferable")

var shardQuotaPools = struct {
	sync.Mutex
	next uint64
	ids  map[*SharedQuotaPool]uint64
}{ids: make(map[*SharedQuotaPool]uint64)}

// shardQuotaPoolID identifies an in-process pool without serializing a Go
// pointer into the task image. IDs are used only for handoff validation.
func shardQuotaPoolID(pool *SharedQuotaPool) uint64 {
	shardQuotaPools.Lock()
	defer shardQuotaPools.Unlock()
	if id := shardQuotaPools.ids[pool]; id != 0 {
		return id
	}
	shardQuotaPools.next++
	id := shardQuotaPools.next
	shardQuotaPools.ids[pool] = id
	return id
}

type shardIO struct {
	s *ShardPolicy
}

func (i shardIO) Submit(cmd Cmd) error {
	if cmd.Cpu < 0 || int(cmd.Cpu) >= len(i.s.global) {
		return fmt.Errorf("foreign CPU %d", cmd.Cpu)
	}
	if cmd.Kind == CmdDispatch && i.s.suppressPID != 0 && cmd.Pid == i.s.suppressPID {
		return ErrTaskNotTransferable // Policy.queueRetry retains the pending task
	}
	cmd.Cpu = i.s.global[cmd.Cpu]
	return i.s.io.Submit(cmd)
}
func (i shardIO) Idle(cpu int32) bool     { return i.s.io.Idle(i.s.global[cpu]) }
func (i shardIO) CurrPid(cpu int32) int32 { return i.s.io.CurrPid(i.s.global[cpu]) }
func (i shardIO) RunStart(cpu int32) uint64 {
	if r, ok := i.s.io.(runStartIO); ok {
		return r.RunStart(i.s.global[cpu])
	}
	return 0
}
func (i shardIO) SetQueued(cpu int32, n, keep uint64) {
	i.s.io.SetQueued(i.s.global[cpu], n, keep)
}
func (i shardIO) Affinity(pid int32) ([]int32, error) {
	a, ok := i.s.io.(AffinityIO)
	if !ok {
		return nil, errors.New("affinity unavailable")
	}
	global, err := a.Affinity(pid)
	if err != nil {
		return nil, err
	}
	local := make([]int32, 0, len(global))
	for _, cpu := range global {
		if c, ok := i.s.local[cpu]; ok {
			local = append(local, c)
		}
	}
	return local, nil
}
func (i shardIO) Available(cpu int32) uint64 {
	if c, ok := i.s.io.(CapacityIO); ok {
		return c.Available(i.s.global[cpu])
	}
	return 1024
}
func (i shardIO) Publish(slot uint32, pub TaskPub) {
	if p, ok := i.s.io.(publishIO); ok {
		p.Publish(slot, pub)
	}
}

type shardSeqIO struct {
	shardIO
	seq runSeqIO
}

func (i shardSeqIO) RunSeq(cpu int32) uint64 {
	return i.seq.RunSeq(i.s.global[cpu])
}

// NewShardPolicy creates a genuinely independent Policy for the supplied
// physical CPUs. A separate per-shard command writer must be passed as io.
// CPU IDs and the list must not be changed after construction.
func NewShardPolicy(io IO, cpus []int32, enums Enums) (*ShardPolicy, error) {
	if io == nil || len(cpus) == 0 {
		return nil, errors.New("shard needs IO and at least one CPU")
	}
	s := &ShardPolicy{global: append([]int32(nil), cpus...), local: make(map[int32]int32),
		io: io, stops: make(map[int32]shardStop)}
	for c, global := range cpus {
		if global < 0 {
			return nil, fmt.Errorf("invalid CPU %d", global)
		}
		if _, exists := s.local[global]; exists {
			return nil, fmt.Errorf("duplicate CPU %d", global)
		}
		s.local[global] = int32(c)
	}
	var localIO IO = shardIO{s}
	if seq, ok := io.(runSeqIO); ok {
		localIO = shardSeqIO{shardIO: shardIO{s}, seq: seq}
	}
	s.Policy = New(localIO, len(cpus), enums)
	return s, nil
}

// Handle maps an event's CPU fields to this Policy's local CPU space.
// A task's globally unique BPF slot is used for core publications; the
// per-Policy slot allocator is never allowed to allocate overlapping slots.
func (s *ShardPolicy) Handle(slot int32, ev Event) error {
	if ev.Kind == EvIdle {
		local, ok := s.local[ev.Cpu]
		if !ok {
			return fmt.Errorf("foreign idle CPU %d", ev.Cpu)
		}
		ev.Cpu = local
	} else {
		if slot <= 0 || slot >= int32(MaxTaskSlots) {
			return fmt.Errorf("invalid task slot %d", slot)
		}
		if ev.Kind == EvEnqueue {
			local, ok := s.local[ev.Cpu]
			if !ok {
				return fmt.Errorf("foreign enqueue CPU %d", ev.Cpu)
			}
			ev.Cpu = local
			if s.Policy.tasks[ev.Pid] == nil {
				s.Policy.tasks[ev.Pid] = &Task{Pid: ev.Pid, Tgid: ev.Tgid,
					Cpu: local, Slot: uint32(slot), initial: true}
			}
		}
		if t := s.Policy.tasks[ev.Pid]; t != nil && t.Slot != uint32(slot) {
			return fmt.Errorf("PID %d has slot %d, not %d", ev.Pid, t.Slot, slot)
		}
	}
	if local, ok := s.local[ev.WakerCpu]; ok {
		ev.WakerCpu = local
	} else {
		ev.WakerCpu = -1
	}
	s.Policy.Handle(ev)
	return nil
}

// HandleForTransfer accounts for a kernel enqueue that landed in another
// shard while the source still owns a running task. The source handles it
// against its last local CPU, but cannot redispatch the task there before
// Export removes it from the source pack. The kernel enqueue is the stop
// acknowledgement; merely submitting CMD_STOP is not.
func (s *ShardPolicy) HandleForTransfer(slot int32, ev Event, sourceCPU int32) error {
	if ev.Kind != EvEnqueue || ev.Pid <= 0 {
		return errors.New("remote transfer requires a task enqueue")
	}
	if _, ok := s.local[sourceCPU]; !ok {
		return fmt.Errorf("source CPU %d is not local", sourceCPU)
	}
	ev.Cpu = sourceCPU
	s.suppressPID = ev.Pid
	defer func() { s.suppressPID = 0 }()
	return s.Handle(slot, ev)
}

// CanTransfer distinguishes running state from a detached pending task.
// Shared-quota parked tasks are pending too; their backlog is rebuilt by Import.
func (s *ShardPolicy) CanTransfer(pid, slot int32) bool {
	p := s.Policy
	t := p.tasks[pid]
	if t == nil || t.Slot != uint32(slot) ||
		t.state != Queued && t.state != Sleeping && t.state != Parked ||
		t.Blocked || (t.backlog != nil && t.state != Parked) ||
		(t.state == Parked && t.backlog == nil) {
		return false
	}
	if _, active := p.act.migrate[t]; active {
		return false
	}
	for g := p.group(t); g != nil; g = g.Parent {
		if g.bw != nil && (g.bw.owner == nil || g.bw.owner != p.sharedQuota) {
			return false
		}
	}
	return true
}

// AffinityTarget reports a physical destination when a mask update leaves a
// task with no allowed CPU in this shard. The coordinator must export queued
// state before delivering the cpumask event to the destination owner.
func (s *ShardPolicy) AffinityTarget(pid int32) (int32, bool, error) {
	a, ok := s.io.(AffinityIO)
	if !ok {
		return 0, false, errors.New("CPU affinity unavailable for shard migration")
	}
	mask, err := a.Affinity(pid)
	if err != nil {
		return 0, false, err
	}
	if len(mask) == 0 {
		return 0, false, fmt.Errorf("PID %d has no allowed CPU", pid)
	}
	for _, cpu := range mask {
		if _, ok := s.local[cpu]; ok {
			return 0, false, nil
		}
	}
	return mask[0], true, nil
}

type shardRavg struct{ Val, At, Cur float64 }

func saveRavg(r Ravg) shardRavg   { return shardRavg{r.val, r.at, r.cur} }
func (r shardRavg) restore() Ravg { return Ravg{r.Val, r.At, r.Cur} }

type shardTaskImage struct {
	Pid, Tgid, CPU                          int32
	Slot                                    uint32
	EnqSeq, Weight, Nice, Cgid, CustomSlice uint64
	Policy                                  uint32
	Allowed                                 []int32
	AffinityKnown                           bool
	GroupConfigured                         bool
	PoolID                                  uint64
	Groups                                  []shardGroupImage
	Vruntime                                uint64
	Req                                     Request
	Lag                                     int64
	Initial                                 bool
	Blocked                                 bool
	LastSum                                 uint64
	Queued                                  bool
	Run                                     shardRavg
	UtilEst, RunnableEst, WokeAt, SleptAt   uint64
	WakeFlips, WakeDecayAt                  uint64
	LastWakee                               int32
}

type shardGroupImage struct {
	ID, Weight, Quota, Period uint64
	HasQuota                  bool
	Idle, Root                bool
}

// Export removes a queued, quota-parked, or sleeping task from its source policy, preserving
// virtual lag and PELT task history. Running tasks cannot be exported; tasks
// with quotas require a shared pool on every ancestor.
// On failure the source remains unchanged.
func (s *ShardPolicy) Export(pid, slot int32) ([]byte, error) {
	p := s.Policy
	t := p.tasks[pid]
	if t == nil || t.Slot != uint32(slot) {
		return nil, fmt.Errorf("PID %d slot %d is not local", pid, slot)
	}
	if t.state != Queued && t.state != Sleeping && t.state != Parked {
		return nil, fmt.Errorf("%w: PID %d is not queued, parked, or sleeping", ErrTaskNotTransferable, pid)
	}
	if _, active := p.act.migrate[t]; active || (t.backlog != nil && t.state != Parked) ||
		(t.state == Parked && t.backlog == nil) || t.Blocked {
		return nil, fmt.Errorf("%w: PID %d has active, proxy, or bandwidth state", ErrTaskNotTransferable, pid)
	}
	img := shardTaskImage{
		Pid: t.Pid, Tgid: t.Tgid, CPU: s.global[t.Cpu], Slot: t.Slot,
		EnqSeq: t.EnqSeq, Weight: t.Weight, Nice: t.Nice, Cgid: t.Cgid,
		CustomSlice: t.CustomSlice, Policy: t.Policy, Vruntime: t.Vruntime,
		Req: t.Req, Lag: t.lag, Initial: t.initial, Blocked: t.Blocked,
		LastSum:         t.lastSum,
		Queued:          t.state == Queued || t.state == Parked,
		GroupConfigured: p.F.Cgroups && p.groups[t.Cgid] != nil,
		WakeFlips:       t.wake.flips, WakeDecayAt: t.wake.decayAt, LastWakee: t.wake.lastWakee,
	}
	for g := p.group(t); g != nil; g = g.Parent {
		group := shardGroupImage{ID: g.ID, Weight: g.Weight, Idle: g.Idle, Root: g.root}
		if b := g.bw; b != nil {
			if b.owner == nil || b.owner != p.sharedQuota {
				return nil, fmt.Errorf("%w: PID %d has unshared quota state", ErrTaskNotTransferable, pid)
			}
			group.HasQuota, group.Quota, group.Period = true, b.quota, b.period
			img.PoolID = shardQuotaPoolID(b.owner)
		}
		img.Groups = append(img.Groups, group)
	}
	for _, c := range t.Allowed {
		img.Allowed = append(img.Allowed, s.global[c])
	}
	if a, ok := s.io.(AffinityIO); ok {
		if allowed, err := a.Affinity(pid); err == nil {
			img.Allowed = append([]int32(nil), allowed...)
			img.AffinityKnown = true
		}
	}
	if st := p.ld.tasks[t]; st != nil {
		img.Run = saveRavg(st.run)
		img.UtilEst, img.RunnableEst, img.WokeAt, img.SleptAt =
			st.utilEst, st.runnableEst, st.wokeAt, st.sleptAt
	}
	if t.delay.pack != nil {
		d := t.delay
		if d.pack.emptyGen != d.gen {
			img.Lag = 0
		} else {
			img.Lag = delayDebt(t.lag, int64(d.pack.Ref-d.ref), d.sum, t.Weight)
		}
	}
	if t.pack != nil {
		img.Lag = t.pack.lag(t, max(p.request(t), p.Slice)+p.TickNs)
	}
	var b bytes.Buffer
	if err := gob.NewEncoder(&b).Encode(img); err != nil {
		return nil, err
	}
	// All failure checks and serialization are complete before detachment.
	p.removeQueued(t)
	p.removeBacklog(t)
	if t.pack != nil {
		t.pack.leave(t, true)
		p.recomputeWeights(t.Cpu)
	}
	delete(p.ld.tasks, t)
	delete(p.lastStop, t)
	delete(p.tasks, pid)
	delete(s.stops, pid)
	if pub, ok := p.io.(publishIO); ok {
		pub.Publish(t.Slot, TaskPub{})
	}
	return b.Bytes(), nil
}

// Import validates the complete image before making the task visible to the
// destination. An already-queued task is placed and enqueued by this owner;
// a sleeping task remains asleep until its next kernel enqueue event.
func (s *ShardPolicy) Import(pid, slot, cpu int32, image []byte) error {
	local, ok := s.local[cpu]
	if !ok {
		return fmt.Errorf("foreign destination CPU %d", cpu)
	}
	if s.Policy.tasks[pid] != nil {
		return fmt.Errorf("PID %d already belongs to destination", pid)
	}
	var img shardTaskImage
	if err := gob.NewDecoder(bytes.NewReader(image)).Decode(&img); err != nil {
		return err
	}
	if img.Pid != pid || img.Slot != uint32(slot) || img.Weight == 0 {
		return errors.New("task image identity or weight mismatch")
	}
	if !img.AffinityKnown && img.CPU != cpu {
		return fmt.Errorf("PID %d cannot migrate without a verified affinity mask", pid)
	}
	p := s.Policy
	if img.GroupConfigured != (p.F.Cgroups && p.groups[img.Cgid] != nil) {
		return fmt.Errorf("destination cgroup %d configuration differs for PID %d", img.Cgid, pid)
	}
	g := p.groups[img.Cgid]
	if !p.F.Cgroups {
		g = nil
	}
	for _, ancestor := range img.Groups {
		if g == nil || g.ID != ancestor.ID || g.Weight != ancestor.Weight ||
			g.Idle != ancestor.Idle || g.root != ancestor.Root ||
			(g.bw != nil) != ancestor.HasQuota {
			return fmt.Errorf("destination cgroup ancestry differs for PID %d", pid)
		}
		if ancestor.HasQuota {
			b := g.bw
			if p.sharedQuota == nil || img.PoolID == 0 ||
				shardQuotaPoolID(p.sharedQuota) != img.PoolID ||
				b.owner != p.sharedQuota || b.shared == nil ||
				b.quota != ancestor.Quota || b.period != ancestor.Period {
				return fmt.Errorf("destination quota pool differs for PID %d", pid)
			}
		}
		g = g.Parent
	}
	if g != nil || len(img.Groups) == 0 && img.PoolID != 0 {
		return fmt.Errorf("destination cgroup ancestry incomplete for PID %d", pid)
	}
	if img.AffinityKnown {
		permitted := false
		for _, c := range img.Allowed {
			permitted = permitted || c == cpu
		}
		if !permitted {
			return fmt.Errorf("PID %d is not allowed on CPU %d", pid, cpu)
		}
	}
	allowed := make([]int32, 0, len(img.Allowed))
	for _, c := range img.Allowed {
		if l, ok := s.local[c]; ok {
			allowed = append(allowed, l)
		}
	}
	if len(img.Allowed) != 0 && len(allowed) == 0 {
		return fmt.Errorf("PID %d has no affinity in destination shard", pid)
	}
	t := &Task{Pid: pid, Tgid: img.Tgid, Cpu: local, Slot: uint32(slot),
		EnqSeq: img.EnqSeq, Weight: img.Weight, Nice: img.Nice, Cgid: img.Cgid,
		CustomSlice: img.CustomSlice, Policy: img.Policy, Allowed: allowed,
		Vruntime: img.Vruntime, Req: img.Req, lag: img.Lag,
		initial: img.Initial, Blocked: img.Blocked,
		lastSum: img.LastSum, state: Sleeping}
	t.wake = wakeState{flips: img.WakeFlips, decayAt: img.WakeDecayAt, lastWakee: img.LastWakee}
	p.tasks[pid] = t
	p.ld.tasks[t] = &taskLoadState{run: img.Run.restore(), utilEst: img.UtilEst,
		runnableEst: img.RunnableEst, wokeAt: img.WokeAt, sleptAt: img.SleptAt}
	if img.Queued {
		p.placeTask(t, local, false)
		p.recomputeWeights(local)
		if p.throttled(t) {
			p.park(t)
		} else {
			p.queue(t, local, false)
		}
	} else {
		p.pub.PublishTask(p, t, p.now)
	}
	return nil
}

// Steal returns a queued affinity-compatible task on a physical source CPU.
// Export is performed by the coordinator after this selection.
func (s *ShardPolicy) Steal(from, dst int32, level int) (pid, slot int32, ok bool, err error) {
	local, owned := s.local[from]
	if !owned {
		return 0, 0, false, fmt.Errorf("foreign source CPU %d", from)
	}
	a, supported := s.io.(AffinityIO)
	if !supported {
		return 0, 0, false, errors.New("cross-shard steal requires affinity IO")
	}
	var chosen *Task
	s.Policy.trees[local].firstN(taskScan, func(t *Task) bool {
		if t.Blocked {
			return false
		}
		cpus, err := a.Affinity(t.Pid)
		if err != nil {
			return false
		}
		for _, c := range cpus {
			if c == dst {
				chosen = t
				return true
			}
		}
		return false
	})
	if chosen == nil {
		return 0, 0, false, nil
	}
	return chosen.Pid, int32(chosen.Slot), true, nil
}

// StopFor requests preemption only for a task currently running on the source
// CPU. A successful command is not an acknowledgement; StopTarget waits for a
// newer enqueue before the coordinator may export the task.
func (s *ShardPolicy) StopFor(cpu, pid, dst int32) error {
	local, owned := s.local[cpu]
	if !owned || dst < 0 || pid <= 0 {
		return ErrTaskNotTransferable
	}
	if _, same := s.local[dst]; same {
		return ErrTaskNotTransferable
	}
	t := s.Policy.tasks[pid]
	if t == nil || t.Slot == 0 || t.Cpu != local || t.state != Dispatched ||
		s.Policy.cpus[local].task != t || s.io.CurrPid(cpu) != pid {
		return ErrTaskNotTransferable
	}
	if _, migrating := s.Policy.act.migrate[t]; migrating {
		return ErrTaskNotTransferable
	}
	if _, pending := s.stops[pid]; pending {
		return ErrTaskNotTransferable
	}
	a, ok := s.io.(AffinityIO)
	if !ok {
		return ErrTaskNotTransferable
	}
	mask, err := a.Affinity(pid)
	if err != nil {
		return fmt.Errorf("%w: cannot verify STOP destination affinity: %v", ErrTaskNotTransferable, err)
	}
	allowed := false
	for _, c := range mask {
		allowed = allowed || c == dst
	}
	if !allowed {
		return ErrTaskNotTransferable
	}
	if err := s.io.Submit(Cmd{Kind: CmdStop, Pid: pid, Cpu: cpu, Slot: t.Slot}); err != nil {
		return fmt.Errorf("%w: STOP submit failed: %v", ErrTaskNotTransferable, err)
	}
	s.stops[pid] = shardStop{slot: int32(t.Slot), dst: dst, seq: t.EnqSeq}
	return nil
}

// StopTarget reports an acknowledged stop only when the kernel has enqueued a
// newer generation of the same task and the destination is still permitted.
// A duplicate event from the old generation must not be used as an ack.
func (s *ShardPolicy) StopTarget(pid, slot int32, seq uint64) (dst int32, stale, acknowledged bool) {
	stop, ok := s.stops[pid]
	if !ok || stop.slot != slot {
		return 0, false, false
	}
	if seq <= stop.seq {
		return 0, true, false
	}
	delete(s.stops, pid)
	a, ok := s.io.(AffinityIO)
	if !ok {
		return 0, false, false
	}
	mask, err := a.Affinity(pid)
	if err != nil {
		return 0, false, false
	}
	for _, c := range mask {
		if c == stop.dst {
			return stop.dst, false, true
		}
	}
	return 0, false, false
}

func (s *ShardPolicy) ClearStop(pid int32) {
	delete(s.stops, pid)
}

// Tick schedules local CPUs only; call Periodic separately when advancing
// quota/heartbeat time for each Policy.
func (s *ShardPolicy) Tick() { s.Policy.Schedule() }

// ShardStats are one physical CPU's policy statistics for snapshot publishing.
type ShardStats struct {
	Load, Util, W, Queued, BalanceCap, WakeLoad uint64
}

func (s *ShardPolicy) Snapshot(cpu int32) ShardStats {
	local, ok := s.local[cpu]
	if !ok {
		return ShardStats{}
	}
	p := s.Policy
	cap := p.topo.Capacity[local]
	if provider, ok := s.io.(CapacityIO); ok {
		cap = max(cap*provider.Available(cpu)/1024, 1)
	}
	return ShardStats{
		Load: max(p.ld.cpuLoad(local, p.now), p.packs[local].W),
		Util: p.ld.cpuUtil(local, p.now), W: p.packs[local].W,
		Queued: uint64(p.trees[local].Len()), BalanceCap: cap,
		WakeLoad: p.ld.cpuWakeLoad(local),
	}
}
