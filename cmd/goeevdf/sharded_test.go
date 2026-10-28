package main

import (
	"reflect"
	"strings"
	"testing"

	"github.com/Gthulhu/scx_goeevdf/eevdf"
	"github.com/Gthulhu/scx_goeevdf/internal/bpf"
	"github.com/Gthulhu/scx_goeevdf/internal/shard"
)

type shardFrequencyIO struct{}

func (shardFrequencyIO) Submit(eevdf.Cmd) error          { return nil }
func (shardFrequencyIO) Idle(int32) bool                 { return false }
func (shardFrequencyIO) CurrPid(int32) int32             { return 0 }
func (shardFrequencyIO) SetQueued(int32, uint64, uint64) {}
func (shardFrequencyIO) Affinity(int32) ([]int32, error) { return []int32{2}, nil }

func TestShardAttachFrequencyBoost(t *testing.T) {
	for _, enabled := range []bool{true, false} {
		t.Run(map[bool]string{true: "enabled", false: "disabled"}[enabled], func(t *testing.T) {
			adapter, err := shard.NewPolicyEngine(shardFrequencyIO{}, []int32{2}, eevdf.Enums{EnqWakeup: 1})
			if err != nil {
				t.Fatal(err)
			}
			adapter.Shard.Policy.F.Cpufreq = enabled
			s := &bpf.Sched{CPU: make([]bpf.CpuSlot, 3), Ctl: make([]bpf.CpuCtl, 3)}
			m := &managedShard{
				PolicyEngine: adapter, sched: s,
				governor: eevdf.NewFrequencyGovernor(3, 10_000_000),
				cpus:     []int32{2}, samples: make([]eevdf.StatSample, 3),
				nextTick: ^uint64(0), nextStat: ^uint64(0),
				nextQuotaCheck: ^uint64(0),
			}
			if err := m.Handle(3, eevdf.Event{
				Kind: eevdf.EvEnqueue, Pid: 42, Tgid: 42, Cpu: 2,
				Now: clockNow(), Weight: 1024, EnqSeq: 1,
			}); err != nil {
				t.Fatal(err)
			}
			if !m.newWork[2] {
				t.Fatalf("enqueue did not mark selected physical CPU: %v", m.newWork)
			}
			if err := m.Tick(); err != nil {
				t.Fatal(err)
			}
			want := uint64(0)
			if enabled {
				want = 1024
			}
			if s.Ctl[2].PerfTarget != want || s.Ctl[0].PerfTarget != 0 || m.newWork[2] {
				t.Fatalf("frequency target = %d, unrelated CPU = %d, pending boosts = %v; want %d",
					s.Ctl[2].PerfTarget, s.Ctl[0].PerfTarget, m.newWork, want)
			}
		})
	}
}

func TestShardedRouteUsesCoordinatorProvisionalSlots(t *testing.T) {
	topo, err := shard.NewTopology(2, [][]int32{{0, 1}}, false)
	if err != nil {
		t.Fatal(err)
	}
	engine := &noOpShardEngine{}
	c, err := shard.New(topo, []shard.Engine{engine})
	if err != nil {
		t.Fatal(err)
	}
	for _, wire := range []bpf.Ev{
		{Kind: bpf.EvEnqueue, Pid: 41, Cpu: 0, Slot: 1},
		{Kind: bpf.EvEnqueue, Pid: 42, Cpu: 0},
		{Kind: bpf.EvEnqueue, Pid: 43, Cpu: 1},
	} {
		if err := c.Route(int32(wire.Slot), shardEvent(wire)); err != nil {
			t.Fatal(err)
		}
	}
	explicit, okExplicit := c.Slot(41)
	a, okA := c.Slot(42)
	b, okB := c.Slot(43)
	if !okExplicit || explicit != 1 || !okA || !okB ||
		a <= 0 || b <= 0 || a == b || a == explicit || b == explicit {
		t.Fatalf("invalid coordinator slots %d/%d/%d (%v/%v/%v)",
			explicit, a, b, okExplicit, okA, okB)
	}
	if err := c.Route(explicit, shardEvent(bpf.Ev{Kind: bpf.EvEnqueue, Pid: 44, Cpu: 0})); err == nil {
		t.Fatal("allocated another task's explicit BPF slot")
	}
	if _, err := c.Step(0); err != nil {
		t.Fatal(err)
	}
	if err := c.Route(0, shardEvent(bpf.Ev{Kind: bpf.EvSleep, Pid: 42})); err != nil {
		t.Fatalf("zero-slot event before first dispatch: %v", err)
	}
	if err := c.Route(a+1, shardEvent(bpf.Ev{Kind: bpf.EvKeep, Pid: 42})); err == nil {
		t.Fatal("accepted BPF slot different from coordinator assignment")
	}
	if _, err := c.Step(0); err != nil {
		t.Fatal(err)
	}
	if engine.handledSlot != a {
		t.Fatalf("policy received slot %d, want coordinator slot %d", engine.handledSlot, a)
	}
}

func TestCgroupChangeMailboxAppliedByOwner(t *testing.T) {
	s := &bpf.Sched{CPU: make([]bpf.CpuSlot, 1), Ctl: make([]bpf.CpuCtl, 1)}
	pressure := eevdf.NewPressure(1)
	engine, err := shard.NewPolicyEngine(shardBackend{backend{sched: s, pressure: pressure}, 0},
		[]int32{0}, eevdf.Enums{})
	if err != nil {
		t.Fatal(err)
	}
	m := &managedShard{
		PolicyEngine: engine, sched: s, pressure: pressure,
		governor: eevdf.NewFrequencyGovernor(1, 0),
		cpus:     []int32{0}, samples: make([]eevdf.StatSample, 1),
		nextStat: ^uint64(0), nextTick: ^uint64(0), nextQuotaCheck: ^uint64(0),
		groups: groupSync{
			paths:   map[uint64]string{8: "/sys/fs/cgroup/removed"},
			applied: map[uint64]groupConfig{8: {}},
		},
	}
	topo, err := shard.NewTopology(1, [][]int32{{0}}, false)
	if err != nil {
		t.Fatal(err)
	}
	c, err := shard.New(topo, []shard.Engine{m})
	if err != nil {
		t.Fatal(err)
	}
	b := &groupBroadcaster{paths: map[uint64]string{8: "/sys/fs/cgroup/removed"},
		applied: map[uint64]groupConfig{8: {}}}
	if err := b.event(c, bpf.EvCgrpExit, 8); err != nil {
		t.Fatal(err)
	}
	if len(m.groups.paths) != 1 {
		t.Fatal("broadcast applied configuration outside its owner step")
	}
	if len(b.paths) != 0 {
		t.Fatal("exit left a stale global cgroup path")
	}
	if _, err := c.Step(0); err != nil {
		t.Fatal(err)
	}
	if len(m.groups.paths) != 0 || len(m.groups.applied) != 0 {
		t.Fatalf("exit not broadcast to owner: %+v", m.groups)
	}
}

func TestManagedShardConfigureKeepsGroupCacheCoherent(t *testing.T) {
	pool := eevdf.NewSharedQuotaPool()
	engine, err := shard.NewPolicyEngineWithQuota(shardFrequencyIO{}, []int32{0}, eevdf.Enums{}, pool)
	if err != nil {
		t.Fatal(err)
	}
	m := &managedShard{PolicyEngine: engine, groups: groupSync{
		applied: map[uint64]groupConfig{7: {weight: 100, period: 1_000_000}},
		paths:   map[uint64]string{7: "/sys/fs/cgroup/existing"},
	}}
	for _, cfg := range []shard.Config{
		{Kind: shard.ConfigGroup, ID: 7, Weight: 200},
		{Kind: shard.ConfigGroup, ID: 8, Parent: 7, Weight: 300},
	} {
		if err := m.Configure(cfg); err != nil {
			t.Fatal(err)
		}
	}
	if got := m.groups.applied[7]; got.weight != 200 || got.period != 1_000_000 {
		t.Fatalf("weight update lost prior quota: %+v", got)
	}
	if err := pool.Configure(8, 10_000, 1_000_000); err != nil {
		t.Fatal(err)
	}
	if err := m.Configure(shard.Config{Kind: shard.ConfigQuota, ID: 8, Quota: 10_000, Period: 1_000_000}); err != nil {
		t.Fatal(err)
	}
	if got := m.groups.applied[8]; got.parent != 7 || got.weight != 300 || got.quota != 10_000 {
		t.Fatalf("quota update lost group configuration: %+v", got)
	}
	if err := m.Configure(shard.Config{Kind: shard.ConfigQuota, ID: 8, Period: 1_000_000}); err == nil {
		t.Fatal("local cache must not accept a quota change rejected by the shared pool")
	}
	if m.groups.applied[8].quota != 10_000 {
		t.Fatal("failed quota update changed local cache")
	}
	if err := pool.Configure(8, 0, 0); err != nil {
		t.Fatal(err)
	}
	if err := m.Configure(shard.Config{Kind: shard.ConfigQuota, ID: 8, Period: 1_000_000}); err != nil {
		t.Fatal(err)
	}
	if got := m.groups.applied[8]; got.quota != 0 || got.period != 1_000_000 {
		t.Fatalf("quota disable left stale cache: %+v", got)
	}
	if m.groups.paths[7] != "/sys/fs/cgroup/existing" {
		t.Fatal("broadcast removed an existing group path")
	}
	if err := m.Configure(shard.Config{Kind: shard.ConfigRemoveGroup, ID: 8}); err != nil {
		t.Fatal(err)
	}
	if _, ok := m.groups.applied[8]; ok {
		t.Fatal("group removal left cached configuration")
	}
}

func TestGroupSyncRecordsPathAfterBroadcastCacheHit(t *testing.T) {
	const root = "/sys/fs/cgroup"
	id, err := cgroupID(root)
	if err != nil {
		t.Skipf("cgroupfs unavailable: %v", err)
	}
	config := groupConfig{weight: readWeight(root)}
	if quota, period, err := readQuota(root); err == nil {
		config.quota, config.period = quota, period
	}
	s := groupSync{applied: map[uint64]groupConfig{id: config}}
	p := eevdf.New(shardFrequencyIO{}, 1, eevdf.Enums{})
	if err := s.configure(p, root); err != nil {
		t.Fatal(err)
	}
	if s.paths[id] != root {
		t.Fatal("matching broadcast cache entry prevented recording the lazy group path")
	}
}

func TestShardedQuotaPreflightFailure(t *testing.T) {
	if _, err := readShardQuotaSnapshot("cmd/goeevdf/no-such-cgroup"); err == nil {
		t.Fatal("accepted unreadable cgroup tree")
	}
	snapshot := &shardQuotaSnapshot{groups: map[uint64]shardQuota{
		5: {quota: 10_000_000, period: 100_000_000},
		6: {quota: 0, period: 100_000_000},
	}}
	for _, tt := range []struct {
		id, quota, period uint64
		want              bool
	}{
		{5, 10_000_000, 100_000_000, true},
		{5, 20_000_000, 100_000_000, false},
		{5, 0, 100_000_000, false},
		{6, 0, 100_000_000, true},
		{7, 0, 100_000_000, true},
		{7, 10_000_000, 100_000_000, false},
	} {
		if got := snapshot.permits(tt.id, tt.quota, tt.period); got != tt.want {
			t.Fatalf("quota id=%d quota=%d period=%d permitted=%v, want %v",
				tt.id, tt.quota, tt.period, got, tt.want)
		}
		err := snapshot.require(tt.id, tt.quota, tt.period, "/sys/fs/cgroup/example")
		if tt.want && err != nil {
			t.Fatalf("permitted quota id=%d rejected: %v", tt.id, err)
		}
		if !tt.want && (err == nil || !strings.Contains(err.Error(), "dynamic cpu.max")) {
			t.Fatalf("changed quota id=%d accepted: %v", tt.id, err)
		}
	}
}

func TestShardedQuotaStartupRequiresConfiguredSnapshot(t *testing.T) {
	const root = "/sys/fs/cgroup"
	id, err := cgroupID(root)
	if err != nil {
		t.Skipf("cgroupfs unavailable: %v", err)
	}
	quota, period, err := readQuota(root)
	if err != nil {
		t.Skipf("cpu.max unavailable: %v", err)
	}
	snapshot := &shardQuotaSnapshot{
		paths: []string{root}, groups: map[uint64]shardQuota{id: {quota, period}},
	}
	owners := []*managedShard{
		{groups: groupSync{applied: map[uint64]groupConfig{id: {quota: quota + 1, period: period}}}},
		{groups: groupSync{applied: map[uint64]groupConfig{id: {quota: quota + 1, period: period}}}},
	}
	if _, err := newGroupBroadcaster(snapshot, owners); err == nil {
		t.Fatal("attached with a startup quota different from the snapshot")
	}
	for _, owner := range owners {
		owner.groups.applied[id] = groupConfig{quota: quota, period: period}
	}
	if _, err := newGroupBroadcaster(snapshot, owners); err != nil {
		t.Fatalf("matching startup quota rejected: %v", err)
	}
}

func TestShardedRuntimeQuotaChangeFailsBeforeBroadcastOrDispatch(t *testing.T) {
	const root = "/sys/fs/cgroup"
	id, err := cgroupID(root)
	if err != nil {
		t.Skipf("cgroupfs unavailable: %v", err)
	}
	quota, period, err := readQuota(root)
	if err != nil {
		t.Skipf("cpu.max unavailable: %v", err)
	}
	previous := quota + 1
	if quota == ^uint64(0) {
		previous = quota - 1
	}
	snapshot := &shardQuotaSnapshot{groups: map[uint64]shardQuota{
		id: {quota: previous, period: period},
	}}
	m := &managedShard{multi: true, quotaSnapshot: snapshot}
	if err := m.checkQuota(root); err == nil || !strings.Contains(err.Error(), "dynamic cpu.max") {
		t.Fatalf("owner accepted changed cpu.max: %v", err)
	}
	b := &groupBroadcaster{
		paths:    map[uint64]string{id: root},
		applied:  map[uint64]groupConfig{id: {quota: previous, period: period}},
		snapshot: snapshot,
	}
	// A cached path skips the pre-route broadcast, but its owner must still
	// reject the live change. The bandwidth event must reject it as well.
	if err := b.beforeEnqueue(nil, 1, id); err != nil {
		t.Fatalf("cached path unexpectedly required a broadcast: %v", err)
	}
	if err := b.event(nil, bpf.EvCgrpBW, id); err == nil || !strings.Contains(err.Error(), "dynamic cpu.max") {
		t.Fatalf("bandwidth event accepted changed cpu.max: %v", err)
	}
	if got := b.applied[id]; got.quota != previous {
		t.Fatalf("rejected bandwidth event updated cached quota: %+v", got)
	}
}

func TestShardedNewFiniteGroupFailsBeforeAnnouncement(t *testing.T) {
	const root = "/sys/fs/cgroup"
	quota, _, err := readQuota(root)
	if err != nil {
		t.Skipf("cpu.max unavailable: %v", err)
	}
	if quota == 0 {
		t.Skip("cgroupfs root has no finite quota to emulate a new finite group")
	}
	snapshot := &shardQuotaSnapshot{groups: make(map[uint64]shardQuota)}
	b := &groupBroadcaster{
		paths: make(map[uint64]string), applied: make(map[uint64]groupConfig),
		snapshot: snapshot,
	}
	if err := b.announcePath(nil, root); err == nil || !strings.Contains(err.Error(), "dynamic cpu.max") {
		t.Fatalf("new finite group reached broadcast: %v", err)
	}
	if len(b.paths) != 0 || len(b.applied) != 0 {
		t.Fatal("rejected group was cached despite failed pre-route check")
	}
}

func TestShardCgroupLookupRacesAreNotFatal(t *testing.T) {
	m := &managedShard{multi: true, quotaSnapshot: &shardQuotaSnapshot{
		groups: make(map[uint64]shardQuota),
	}}
	if err := m.syncGroup(2147483647, 99); err != nil {
		t.Fatalf("short-lived task enqueue: %v", err)
	}
	if err := m.syncImportGroup(2147483647); err != nil {
		t.Fatalf("short-lived task import: %v", err)
	}
	if err := m.configureGroup("/sys/fs/cgroup/no-such-shard-group", 99); err != nil {
		t.Fatalf("removed cgroup: %v", err)
	}
	root, err := cgroupID("/sys/fs/cgroup")
	if err != nil {
		t.Skipf("cgroupfs unavailable: %v", err)
	}
	if err := m.configureGroup("/sys/fs/cgroup", root+1); err != nil {
		t.Fatalf("changed cgroup inode: %v", err)
	}
	b := &groupBroadcaster{paths: make(map[uint64]string), applied: make(map[uint64]groupConfig)}
	if err := b.beforeEnqueue(nil, 2147483647, 99); err != nil {
		t.Fatalf("short-lived task before route: %v", err)
	}
	b.paths[99] = "/sys/fs/cgroup/no-such-shard-group"
	if err := b.event(nil, bpf.EvCgrpWeight, 99); err != nil {
		t.Fatalf("cgroup deletion before weight event: %v", err)
	}
}

func TestShardCPUs(t *testing.T) {
	got, err := shardCPUs([][]int32{{2, 0}, {3, 1}}, 4)
	if err != nil || !reflect.DeepEqual(got, []uint32{0, 1, 0, 1}) {
		t.Fatalf("owners %v, error %v", got, err)
	}
	for _, tt := range []struct {
		name   string
		groups [][]int32
		want   string
	}{
		{"missing", [][]int32{{0}, {2, 3}}, "missing CPU 1"},
		{"duplicate", [][]int32{{0, 1}, {1, 2, 3}}, "duplicate CPU 1"},
		{"negative", [][]int32{{0, -1}, {1, 2, 3}}, "invalid"},
		{"empty", [][]int32{{0, 1}, {}}, "empty shard"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			_, err := shardCPUs(tt.groups, 4)
			if err == nil || !strings.Contains(err.Error(), tt.want) {
				t.Fatalf("error %v, want %q", err, tt.want)
			}
		})
	}
}

func TestShardLocalTopologyAndWorkerCPU(t *testing.T) {
	topo := eevdf.Topology{
		Core: []int{0, 0, 1, 1}, LLC: []int{0, 0, 1, 1},
		Node: []int{4, 4, 5, 5}, Capacity: []uint64{1000, 900, 800, 700},
	}
	local, err := localTopology(topo, []int32{3, 2})
	if err != nil || !reflect.DeepEqual(local.Core, []int{1, 1}) ||
		!reflect.DeepEqual(local.Node, []int{5, 5}) ||
		!reflect.DeepEqual(local.Capacity, []uint64{700, 800}) {
		t.Fatalf("local topology %+v, error %v", local, err)
	}
	if _, err := localTopology(topo, []int32{4}); err == nil {
		t.Fatal("accepted CPU without topology")
	}
	cpu, err := workerCPU([]int32{1, 3}, []int32{0, 3})
	if err != nil || cpu != 3 {
		t.Fatalf("worker CPU %d, error %v", cpu, err)
	}
	if _, err := workerCPU([]int32{1, 3}, []int32{0, 2}); err == nil {
		t.Fatal("accepted a shard without a permitted CPU")
	}
}

func TestShardEventWireConversion(t *testing.T) {
	wire := bpf.Ev{Kind: bpf.EvEnqueue, Pid: 14, Tgid: 15, Cpu: 2,
		Slot: 47, Flags: 3, Now: 99, SumExec: 10, Weight: 120,
		CustomSlice: 500, WakerPid: 21, WakerCpu: 1, WakeFlags: 12,
		Policy: 4, EnqSeq: 13, Cgid: 42}
	ev := shardEvent(wire)
	if ev.Kind != eevdf.EvEnqueue || ev.Pid != wire.Pid ||
		ev.Cpu != wire.Cpu || ev.WakerCpu != wire.WakerCpu ||
		ev.EnqSeq != wire.EnqSeq || ev.Cgid != wire.Cgid ||
		ev.CustomSlice != wire.CustomSlice || ev.Weight != wire.Weight {
		t.Fatalf("lost wire event fields: %+v", ev)
	}
}

func TestPullRemoteWithoutOtherShard(t *testing.T) {
	topo, err := shard.NewTopology(2, [][]int32{{0, 1}}, false)
	if err != nil {
		t.Fatal(err)
	}
	// No ring, scheduler attachment, or privileged operations are needed.
	c, err := shard.New(topo, []shard.Engine{&noOpShardEngine{}})
	if err != nil {
		t.Fatal(err)
	}
	if source, err := pullRemote(c, eevdf.Topology{}, 0, func(int32) bool { return true }); err != nil || source != -1 {
		t.Fatalf("single-owner pull: source %d, error %v", source, err)
	}
}

func TestPullRemoteRequestsOwnerSteal(t *testing.T) {
	topo, err := shard.NewTopology(65, [][]int32{
		{0, 1, 2, 3}, {4, 5, 6, 7}, {8, 9, 10, 11}, {12, 13, 14, 15},
		{16, 17, 18, 19}, {20, 21, 22, 23}, {24, 25, 26, 27},
		{28, 29, 30, 31}, {32, 33, 34, 35}, {36, 37, 38, 39},
		{40, 41, 42, 43}, {44, 45, 46, 47}, {48, 49, 50, 51},
		{52, 53, 54, 55}, {56, 57, 58, 59}, {60, 61, 62, 63, 64},
	}, false)
	if err != nil {
		t.Fatal(err)
	}
	engines := make([]shard.Engine, len(topo.CPUs))
	source := &noOpShardEngine{queuedCPU: 0}
	engines[0] = source
	for i := 1; i < len(engines); i++ {
		engines[i] = &noOpShardEngine{queuedCPU: -1}
	}
	c, err := shard.New(topo, engines)
	if err != nil {
		t.Fatal(err)
	}
	for id := range engines {
		if _, err := c.Step(id); err != nil {
			t.Fatal(err)
		}
	}
	nodes := make([]int, 65)
	if source, err := pullRemote(c, eevdf.Topology{Core: nodes, Node: nodes}, 1, func(int32) bool { return false }); err != nil || source != -1 {
		t.Fatalf("non-idle pull: source %d, error %v", source, err)
	}
	if _, err := c.Step(0); err != nil {
		t.Fatal(err)
	}
	if source.stealFrom != 0 || source.stealDst != 0 {
		t.Fatal("requested a steal for a non-idle destination")
	}
	if owner, err := pullRemote(c, eevdf.Topology{Core: nodes, Node: nodes}, 1, func(int32) bool { return true }); err != nil || owner != 0 {
		t.Fatalf("remote pull source owner %d, error %v", owner, err)
	}
	if _, err := c.Step(0); err != nil {
		t.Fatal(err)
	}
	if source.stealFrom != 0 || source.stealDst != 4 || source.stealLevel != 1 {
		t.Fatalf("remote request: from=%d dst=%d level=%d", source.stealFrom, source.stealDst, source.stealLevel)
	}
}

func TestMailboxStealStepsWithoutRingEvent(t *testing.T) {
	var llcs [][]int32
	for first := int32(0); first < 65; first += 32 {
		var cpus []int32
		for cpu := first; cpu < first+32 && cpu < 65; cpu++ {
			cpus = append(cpus, cpu)
		}
		llcs = append(llcs, cpus)
	}
	topo, err := shard.NewTopology(65, llcs, false)
	if err != nil {
		t.Fatal(err)
	}
	// The tiny trailing LLC coalesces with its neighbor.
	source := &noOpShardEngine{queuedCPU: 0, selectedPID: 24, selectedSlot: 9}
	dest := &noOpShardEngine{queuedCPU: -1}
	c, err := shard.New(topo, []shard.Engine{source, dest})
	if err != nil {
		t.Fatal(err)
	}
	if err := c.Route(9, eevdf.Event{Kind: eevdf.EvEnqueue, Pid: 24, Cpu: 0}); err != nil {
		t.Fatal(err)
	}
	if _, err := c.Step(0); err != nil {
		t.Fatal(err)
	}
	for id := 0; id < 2; id++ {
		if _, err := c.Step(id); err != nil {
			t.Fatal(err)
		}
	}
	nodes := make([]int, 65)
	src, err := pullRemote(c, eevdf.Topology{Core: nodes, Node: nodes}, 1, func(int32) bool { return true })
	if err != nil || src != 0 {
		t.Fatalf("remote source %d, error %v", src, err)
	}
	// A mailbox-only step (no events consumed from BPF) must count as work:
	// the runner uses this count to set needed_s and kick the destination.
	processed, err := c.Step(src)
	if err != nil || processed == 0 {
		t.Fatalf("mailbox-only step processed %d, error %v", processed, err)
	}
	if _, err := c.Step(1); err != nil {
		t.Fatal(err)
	}
	if owner, ok := c.Owner(24, 9); !ok || owner != 1 {
		t.Fatalf("handoff destination owner %d, registered %v", owner, ok)
	}
}

type noOpShardEngine struct {
	queuedCPU                 int32
	stealFrom, stealDst       int32
	stealLevel                int
	selectedPID, selectedSlot int32
	handledSlot               int32
}

func (e *noOpShardEngine) Handle(slot int32, _ eevdf.Event) error {
	e.handledSlot = slot
	return nil
}
func (*noOpShardEngine) Export(int32, int32) (shard.Image, error)      { return nil, nil }
func (*noOpShardEngine) Import(int32, int32, int32, shard.Image) error { return nil }
func (e *noOpShardEngine) Steal(from, dst int32, level int) (int32, int32, bool, error) {
	e.stealFrom, e.stealDst, e.stealLevel = from, dst, level
	if e.selectedPID != 0 {
		return e.selectedPID, e.selectedSlot, true, nil
	}
	return 0, 0, false, nil
}
func (*noOpShardEngine) StopFor(int32, int32, int32) error { return nil }
func (*noOpShardEngine) Tick() error                       { return nil }
func (e *noOpShardEngine) Snapshot(cpu int32) shard.Values {
	if cpu == e.queuedCPU {
		return shard.Values{Queued: 2}
	}
	return shard.Values{}
}
