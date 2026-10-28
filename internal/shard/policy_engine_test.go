package shard

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/Gthulhu/scx_goeevdf/eevdf"
)

type shardPolicyIO struct {
	mu         sync.Mutex
	allowed    []int32
	commands   []eevdf.Cmd
	queued     map[int32]uint64
	writes     []int32
	idle       bool
	reject     bool
	rejectStop bool
	curr       int32
}

func (i *shardPolicyIO) Submit(cmd eevdf.Cmd) error {
	i.mu.Lock()
	defer i.mu.Unlock()
	if i.reject && cmd.Kind == eevdf.CmdDispatch || i.rejectStop && cmd.Kind == eevdf.CmdStop {
		return errors.New("dispatch temporarily unavailable")
	}
	i.commands = append(i.commands, cmd)
	return nil
}
func (i *shardPolicyIO) Idle(int32) bool     { return i.idle }
func (i *shardPolicyIO) CurrPid(int32) int32 { return i.curr }
func (i *shardPolicyIO) SetQueued(cpu int32, n, _ uint64) {
	i.mu.Lock()
	defer i.mu.Unlock()
	i.queued[cpu] = n
	i.writes = append(i.writes, cpu)
}
func (i *shardPolicyIO) Affinity(int32) ([]int32, error) {
	return append([]int32(nil), i.allowed...), nil
}

func TestRealPolicyEnginesHandoffAndSteal(t *testing.T) {
	topo, err := NewTopology(80, [][]int32{
		cpusFrom(0, 40), cpusFrom(40, 40),
	}, false)
	if err != nil {
		t.Fatal(err)
	}
	engines := make([]Engine, 2)
	ios := make([]*shardPolicyIO, 2)
	for id := range engines {
		ios[id] = &shardPolicyIO{queued: make(map[int32]uint64), allowed: cpusFrom(0, 80), reject: true}
		engine, err := NewPolicyEngine(ios[id], topo.CPUs[id], eevdf.Enums{EnqWakeup: 1})
		if err != nil {
			t.Fatal(err)
		}
		engines[id] = engine
	}
	c, err := New(topo, engines)
	if err != nil {
		t.Fatal(err)
	}
	for pid := int32(1); pid <= 5; pid++ {
		if err := c.Route(pid, eevdf.Event{Kind: eevdf.EvEnqueue, Pid: pid,
			Cpu: 0, Weight: 100, EnqSeq: 1}); err != nil {
			t.Fatal(err)
		}
	}
	stepAll(t, c)
	if v, ok := c.Snapshot(0); !ok || v.Queued != 5 || v.W != 5*eevdf.Nice0Weight {
		t.Fatalf("source pack weight/EDQ wrong: %+v", v)
	}
	if err := c.Handoff(1, 1, 40); err != nil {
		t.Fatal(err)
	}
	stepAll(t, c)
	stepAll(t, c)
	if id, ok := c.Owner(1, 1); !ok || id != 1 {
		t.Fatal("Policy task not transferred")
	}
	if v, _ := c.Snapshot(0); v.Queued != 4 || v.W != 4*eevdf.Nice0Weight {
		t.Fatalf("source retained migrated task: %+v", v)
	}
	if v, _ := c.Snapshot(40); v.Queued != 1 || v.W != eevdf.Nice0Weight {
		t.Fatalf("destination did not adopt migrated task: %+v", v)
	}
	if ios[0].queued[0] != 4 || ios[1].queued[40] != 1 {
		t.Fatalf("queue writes not mapped to physical CPUs: %v %v", ios[0].queued, ios[1].queued)
	}
	if err := c.Steal(0, 40, 2); err != nil {
		t.Fatal(err)
	}
	stepAll(t, c)
	stepAll(t, c)
	if v, _ := c.Snapshot(40); v.Queued != 2 || v.W != 2*eevdf.Nice0Weight {
		t.Fatalf("steal did not move a queued policy task: %+v", v)
	}
	// A foreign enqueue is exported before the source Policy handles it.
	if err := c.Route(1, eevdf.Event{Kind: eevdf.EvEnqueue, Pid: 1, Cpu: 0, Weight: 100, EnqSeq: 2}); err != nil {
		t.Fatal(err)
	}
	stepAll(t, c)
	stepAll(t, c)
	if id, ok := c.Owner(1, 1); !ok || id != 0 {
		t.Fatal("remote enqueue not routed to physical CPU owner")
	}
}

func TestZeroWireSlotDispatchPublishesDistinctPolicySlots(t *testing.T) {
	topo, _ := NewTopology(80, [][]int32{cpusFrom(0, 40), cpusFrom(40, 40)}, false)
	io0 := &shardPolicyIO{queued: make(map[int32]uint64), allowed: []int32{0, 1}, idle: true}
	io1 := &shardPolicyIO{queued: make(map[int32]uint64), allowed: []int32{40}, idle: true}
	first, _ := NewPolicyEngine(io0, topo.CPUs[0], eevdf.Enums{})
	second, _ := NewPolicyEngine(io1, topo.CPUs[1], eevdf.Enums{})
	c, _ := New(topo, []Engine{first, second})
	for _, ev := range []eevdf.Event{
		{Kind: eevdf.EvEnqueue, Pid: 101, Cpu: 0, Weight: 100},
		{Kind: eevdf.EvEnqueue, Pid: 102, Cpu: 1, Weight: 100},
	} {
		if err := c.Route(0, ev); err != nil {
			t.Fatal(err)
		}
	}
	stepAll(t, c)
	if len(io0.commands) != 2 || io0.commands[0].Slot == 0 ||
		io0.commands[1].Slot == 0 || io0.commands[0].Slot == io0.commands[1].Slot {
		t.Fatalf("dispatch reused provisional BPF slot: %+v", io0.commands)
	}
	for i, pid := range []int32{101, 102} {
		slot := int32(io0.commands[i].Slot)
		if registered, ok := c.Slot(pid); !ok || registered != slot {
			t.Fatalf("PID %d command slot %d != owner slot %d", pid, slot, registered)
		}
		if err := c.Route(slot, eevdf.Event{Kind: eevdf.EvKeep, Pid: pid, Cpu: int32(i)}); err != nil {
			t.Fatal(err)
		}
	}
	stepAll(t, c)
}

func TestDifferentPoliciesPublishDisjointGlobalSlots(t *testing.T) {
	topo, _ := NewTopology(80, [][]int32{cpusFrom(0, 40), cpusFrom(40, 40)}, false)
	io0 := &shardPolicyIO{queued: make(map[int32]uint64), allowed: []int32{0}, idle: true}
	io1 := &shardPolicyIO{queued: make(map[int32]uint64), allowed: []int32{40}, idle: true}
	a, _ := NewPolicyEngine(io0, topo.CPUs[0], eevdf.Enums{})
	b, _ := NewPolicyEngine(io1, topo.CPUs[1], eevdf.Enums{})
	c, _ := New(topo, []Engine{a, b})
	for _, ev := range []eevdf.Event{
		{Kind: eevdf.EvEnqueue, Pid: 201, Cpu: 0, Weight: 100},
		{Kind: eevdf.EvEnqueue, Pid: 202, Cpu: 40, Weight: 100},
	} {
		if err := c.Route(0, ev); err != nil {
			t.Fatal(err)
		}
	}
	stepAll(t, c)
	if len(io0.commands) != 1 || len(io1.commands) != 1 ||
		io0.commands[0].Slot == 0 || io1.commands[0].Slot == 0 ||
		io0.commands[0].Slot == io1.commands[0].Slot {
		t.Fatalf("per-Policy publisher collided in shared BPF task_tbl: %v %v", io0.commands, io1.commands)
	}
	for _, task := range []struct {
		pid  int32
		slot uint32
	}{{201, io0.commands[0].Slot}, {202, io1.commands[0].Slot}} {
		if canonical, ok := c.Slot(task.pid); !ok || uint32(canonical) != task.slot {
			t.Fatalf("PID %d command slot %d != globally allocated slot %d", task.pid, task.slot, canonical)
		}
	}
}

func TestPolicyReceivesZeroWireSlotEventsUntilFirstDispatch(t *testing.T) {
	topo, _ := NewTopology(80, [][]int32{cpusFrom(0, 40), cpusFrom(40, 40)}, false)
	io0 := &shardPolicyIO{queued: make(map[int32]uint64), allowed: []int32{0}, reject: true}
	io1 := &shardPolicyIO{queued: make(map[int32]uint64), allowed: []int32{40}, reject: true}
	a, _ := NewPolicyEngine(io0, topo.CPUs[0], eevdf.Enums{})
	b, _ := NewPolicyEngine(io1, topo.CPUs[1], eevdf.Enums{})
	c, _ := New(topo, []Engine{a, b})
	if err := c.Route(0, eevdf.Event{Kind: eevdf.EvEnqueue, Pid: 44, Cpu: 0, Weight: 100}); err != nil {
		t.Fatal(err)
	}
	stepAll(t, c) // Submit fails; BPF task_ctx.slot is still zero.
	slot, ok := c.Slot(44)
	if !ok || slot <= 0 || len(io0.commands) != 0 {
		t.Fatalf("no provisional slot or dispatch unexpectedly succeeded: %d %v %v", slot, ok, io0.commands)
	}
	for _, ev := range []eevdf.Event{
		{Kind: eevdf.EvKeep, Pid: 44, Cpu: 0},
		{Kind: eevdf.EvSleep, Pid: 44, Cpu: 0},
		{Kind: eevdf.EvEnqueue, Pid: 44, Cpu: 0, Weight: 100},
	} {
		if err := c.Route(0, ev); err != nil {
			t.Fatal("event before first dispatch lost PID ownership:", err)
		}
	}
	stepAll(t, c)
	if id, ok := c.Owner(44, 0); !ok || id != 0 || len(io0.commands) != 0 {
		t.Fatal("zero-slot events incorrectly assigned or dispatched task")
	}
	io0.reject = false
	stepAll(t, c) // first successful DISPATCH installs the canonical slot in BPF
	if len(io0.commands) != 1 || int32(io0.commands[0].Slot) != slot {
		t.Fatalf("first dispatch did not publish provisional slot %d: %+v", slot, io0.commands)
	}
	if err := c.Route(slot, eevdf.Event{Kind: eevdf.EvKeep, Pid: 44, Cpu: 0}); err != nil {
		t.Fatal("BPF slot did not bind after dispatch:", err)
	}
	stepAll(t, c)
}

func TestRealPolicyEngineDefersRunningHandoff(t *testing.T) {
	topo, _ := NewTopology(80, [][]int32{cpusFrom(0, 40), cpusFrom(40, 40)}, false)
	io0 := &shardPolicyIO{queued: make(map[int32]uint64), allowed: cpusFrom(0, 80), idle: true}
	io1 := &shardPolicyIO{queued: make(map[int32]uint64), allowed: cpusFrom(0, 80)}
	a, _ := NewPolicyEngine(io0, topo.CPUs[0], eevdf.Enums{})
	b, _ := NewPolicyEngine(io1, topo.CPUs[1], eevdf.Enums{})
	c, _ := New(topo, []Engine{a, b})
	if err := c.Route(23, eevdf.Event{Kind: eevdf.EvEnqueue, Pid: 23, Cpu: 0, Weight: 100}); err != nil {
		t.Fatal(err)
	}
	stepAll(t, c)
	if len(io0.commands) != 1 || io0.commands[0].Cpu != 0 || io0.commands[0].Slot != 23 {
		t.Fatal("dispatch CPU or global task slot incorrect:", io0.commands)
	}
	if err := c.Handoff(23, 23, 40); err != nil {
		t.Fatal(err)
	}
	if _, err := c.Step(0); err != nil {
		t.Fatal("nontransferable running task killed its owner:", err)
	}
	if id, ok := c.Owner(23, 23); !ok || id != 0 {
		t.Fatal("deferred running handoff lost original owner")
	}
	if err := c.StopFor(0, 23, 40); err != nil {
		t.Fatal(err)
	}
	if _, err := c.Step(0); err != nil {
		t.Fatal(err)
	}
	if len(io0.commands) != 1 {
		t.Fatalf("STOP was sent without kernel current-PID evidence: %+v", io0.commands)
	}
	if err := c.Route(23, eevdf.Event{Kind: eevdf.EvEnqueue, Pid: 23, Cpu: 40,
		Weight: 100, EnqSeq: 2}); err != nil {
		t.Fatal(err)
	}
	stepAll(t, c)
	stepAll(t, c)
	if owner, ok := c.Owner(23, 23); !ok || owner != 1 {
		t.Fatalf("stopped task did not transfer to destination: %d %v", owner, ok)
	}
	if len(io0.commands) != 1 {
		t.Fatalf("source redispatched running task during handoff: %+v", io0.commands)
	}
}

func TestActiveStopRequiresNewKernelEnqueueBeforeTransfer(t *testing.T) {
	topo, _ := NewTopology(80, [][]int32{cpusFrom(0, 40), cpusFrom(40, 40)}, false)
	io0 := &shardPolicyIO{queued: make(map[int32]uint64), allowed: []int32{0, 40}, idle: true}
	io1 := &shardPolicyIO{queued: make(map[int32]uint64), allowed: []int32{0, 40}, idle: true}
	a, _ := NewPolicyEngine(io0, topo.CPUs[0], eevdf.Enums{})
	b, _ := NewPolicyEngine(io1, topo.CPUs[1], eevdf.Enums{})
	c, _ := New(topo, []Engine{a, b})
	if err := c.Route(23, eevdf.Event{Kind: eevdf.EvEnqueue, Pid: 23, Cpu: 0, Weight: 100, EnqSeq: 1}); err != nil {
		t.Fatal(err)
	}
	stepAll(t, c)
	io0.curr = 23
	if err := c.StopFor(0, 23, 40); err != nil {
		t.Fatal(err)
	}
	stepAll(t, c)
	if len(io0.commands) != 2 || io0.commands[1].Kind != eevdf.CmdStop ||
		io0.commands[1].Cpu != 0 || io0.commands[1].Pid != 23 {
		t.Fatalf("STOP was not sent to running source: %+v", io0.commands)
	}
	if owner, ok := c.Owner(23, 23); !ok || owner != 0 {
		t.Fatal("STOP submission prematurely transferred running task")
	}
	if err := c.Route(23, eevdf.Event{Kind: eevdf.EvEnqueue, Pid: 23, Cpu: 0, Weight: 100, EnqSeq: 1}); err != nil {
		t.Fatal(err)
	}
	stepAll(t, c)
	if owner, ok := c.Owner(23, 23); !ok || owner != 0 || len(io0.commands) != 2 {
		t.Fatal("stale enqueue acknowledged STOP")
	}
	if err := c.Route(23, eevdf.Event{Kind: eevdf.EvEnqueue, Pid: 23,
		Cpu: 0, Weight: 100, EnqSeq: 2}); err != nil {
		t.Fatal(err)
	}
	stepAll(t, c)
	if owner, ok := c.Owner(23, 23); !ok || owner != 1 {
		t.Fatalf("acknowledged task not handed off: owner %d, registered %v", owner, ok)
	}
	if len(io0.commands) != 2 || len(io1.commands) != 1 || io1.commands[0].Pid != 23 {
		t.Fatalf("source redispatched or destination did not dispatch: %v / %v", io0.commands, io1.commands)
	}
}

func TestActiveStopFailureAndStaleRequestRetainSource(t *testing.T) {
	topo, _ := NewTopology(80, [][]int32{cpusFrom(0, 40), cpusFrom(40, 40)}, false)
	io0 := &shardPolicyIO{queued: make(map[int32]uint64), allowed: []int32{0, 40},
		idle: true, rejectStop: true}
	io1 := &shardPolicyIO{queued: make(map[int32]uint64), allowed: []int32{0, 40}}
	a, _ := NewPolicyEngine(io0, topo.CPUs[0], eevdf.Enums{})
	b, _ := NewPolicyEngine(io1, topo.CPUs[1], eevdf.Enums{})
	c, _ := New(topo, []Engine{a, b})
	if err := c.Route(23, eevdf.Event{Kind: eevdf.EvEnqueue, Pid: 23, Cpu: 0, Weight: 100, EnqSeq: 1}); err != nil {
		t.Fatal(err)
	}
	stepAll(t, c)
	io0.curr = 23
	if err := c.StopFor(0, 23, 40); err != nil {
		t.Fatal(err)
	}
	stepAll(t, c)
	if owner, ok := c.Owner(23, 23); !ok || owner != 0 || len(io0.commands) != 1 {
		t.Fatal("failed STOP changed owner or submitted a command")
	}
	if err := c.StopFor(40, 23, 0); err == nil {
		t.Fatal("accepted a stop from nonowner CPU")
	}
	io0.rejectStop = false
	if err := c.StopFor(0, 23, 40); err != nil {
		t.Fatal(err)
	}
	if err := c.Route(23, eevdf.Event{Kind: eevdf.EvExit, Pid: 23, Cpu: 0}); err != nil {
		t.Fatal(err)
	}
	stepAll(t, c) // old STOP message is followed by exit
	if _, ok := c.Owner(23, 23); ok {
		t.Fatal("exit retained ownership")
	}
	if len(io0.commands) != 2 {
		t.Fatalf("unexpected STOP submissions: %+v", io0.commands)
	}
}

func TestActiveStopRechecksAffinityAtAcknowledgement(t *testing.T) {
	topo, _ := NewTopology(80, [][]int32{cpusFrom(0, 40), cpusFrom(40, 40)}, false)
	io0 := &shardPolicyIO{queued: make(map[int32]uint64), allowed: []int32{0, 40}, idle: true}
	io1 := &shardPolicyIO{queued: make(map[int32]uint64), allowed: []int32{0, 40}, idle: true}
	a, _ := NewPolicyEngine(io0, topo.CPUs[0], eevdf.Enums{})
	b, _ := NewPolicyEngine(io1, topo.CPUs[1], eevdf.Enums{})
	c, _ := New(topo, []Engine{a, b})
	if err := c.Route(23, eevdf.Event{Kind: eevdf.EvEnqueue, Pid: 23, Cpu: 0, Weight: 100, EnqSeq: 1}); err != nil {
		t.Fatal(err)
	}
	stepAll(t, c)
	io0.curr = 23
	if err := c.StopFor(0, 23, 40); err != nil {
		t.Fatal(err)
	}
	stepAll(t, c)
	io0.allowed = []int32{0}
	if err := c.Route(23, eevdf.Event{Kind: eevdf.EvEnqueue, Pid: 23, Cpu: 0, Weight: 100, EnqSeq: 2}); err != nil {
		t.Fatal(err)
	}
	stepAll(t, c)
	if owner, ok := c.Owner(23, 23); !ok || owner != 0 || len(io1.commands) != 0 {
		t.Fatalf("revoked destination acquired task: owner %d, registered %v, commands %+v", owner, ok, io1.commands)
	}
}

func TestRunningHandoffDoesNotTerminateOwnerLoop(t *testing.T) {
	topo, _ := NewTopology(80, [][]int32{cpusFrom(0, 40), cpusFrom(40, 40)}, false)
	io0 := &shardPolicyIO{queued: make(map[int32]uint64), allowed: cpusFrom(0, 80), idle: true}
	io1 := &shardPolicyIO{queued: make(map[int32]uint64), allowed: cpusFrom(0, 80), reject: true}
	a, _ := NewPolicyEngine(io0, topo.CPUs[0], eevdf.Enums{})
	b, _ := NewPolicyEngine(io1, topo.CPUs[1], eevdf.Enums{})
	c, _ := New(topo, []Engine{a, b})
	if err := c.Route(0, eevdf.Event{Kind: eevdf.EvEnqueue, Pid: 31, Cpu: 0, Weight: 100}); err != nil {
		t.Fatal(err)
	}
	stepAll(t, c) // task is running on CPU 0
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 2)
	for id := 0; id < 2; id++ {
		go func(id int) { done <- c.Run(ctx, id) }(id)
	}
	if err := c.Handoff(31, 0, 40); err != nil {
		t.Fatal(err)
	}
	slot, _ := c.Slot(31)
	if err := c.Route(slot, eevdf.Event{Kind: eevdf.EvEnqueue, Pid: 31, Cpu: 40,
		Weight: 100, EnqSeq: 2}); err != nil {
		t.Fatal(err)
	}
	ticker := time.NewTicker(time.Millisecond)
	defer ticker.Stop()
	timeout := time.After(5 * time.Second)
	for {
		if owner, ok := c.Owner(31, 0); ok && owner == 1 {
			break
		}
		select {
		case err := <-done:
			t.Fatalf("owner loop exited during deferred transfer: %v", err)
		case <-ticker.C:
		case <-timeout:
			t.Fatal("remote enqueue never transferred stopped task")
		}
	}
	cancel()
	for range 2 {
		if err := <-done; err != context.Canceled {
			t.Fatalf("owner loop returned %v", err)
		}
	}
}

func TestAffinityChangeMovesQueuedPolicyTaskToAllowedShard(t *testing.T) {
	topo, _ := NewTopology(80, [][]int32{cpusFrom(0, 40), cpusFrom(40, 40)}, false)
	io0 := &shardPolicyIO{queued: make(map[int32]uint64), allowed: []int32{0, 40}, reject: true}
	io1 := &shardPolicyIO{queued: make(map[int32]uint64), allowed: []int32{0, 40}, reject: true}
	a, _ := NewPolicyEngine(io0, topo.CPUs[0], eevdf.Enums{})
	b, _ := NewPolicyEngine(io1, topo.CPUs[1], eevdf.Enums{})
	c, _ := New(topo, []Engine{a, b})
	if err := c.Route(27, eevdf.Event{Kind: eevdf.EvEnqueue, Pid: 27, Cpu: 0, Weight: 100}); err != nil {
		t.Fatal(err)
	}
	stepAll(t, c)
	io0.allowed = []int32{40}
	io1.allowed = []int32{40}
	if err := c.Route(27, eevdf.Event{Kind: eevdf.EvCpumask, Pid: 27, Cpu: 0}); err != nil {
		t.Fatal(err)
	}
	stepAll(t, c)
	stepAll(t, c)
	if owner, ok := c.Owner(27, 27); !ok || owner != 1 {
		t.Fatalf("cpumask task was not adopted by allowed shard: %d %v", owner, ok)
	}
	if snap, ok := c.Snapshot(40); !ok || snap.W != eevdf.Nice0Weight {
		t.Fatalf("cpumask task missing from destination pack: %+v", snap)
	}
}

func TestSharedCPUControlOnlyReceivesOwnedWrites(t *testing.T) {
	shared := &shardPolicyIO{queued: make(map[int32]uint64),
		allowed: cpusFrom(0, 80), reject: true}
	first, err := NewPolicyEngine(shared, cpusFrom(40, 40), eevdf.Enums{})
	if err != nil {
		t.Fatal(err)
	}
	second, err := NewPolicyEngine(shared, cpusFrom(0, 40), eevdf.Enums{})
	if err != nil {
		t.Fatal(err)
	}
	if len(shared.writes) != 80 {
		t.Fatalf("constructor wrote %d CPU control slots, want 80", len(shared.writes))
	}
	for i, cpu := range shared.writes {
		want := int32(i)
		if i < 40 {
			want = int32(40 + i)
		} else {
			want = int32(i - 40)
		}
		if cpu != want {
			t.Fatalf("constructor write %d touched CPU %d, want owned CPU %d", i, cpu, want)
		}
	}
	if err := first.Handle(7, eevdf.Event{Kind: eevdf.EvEnqueue, Pid: 7, Cpu: 40, Weight: 100}); err != nil {
		t.Fatal(err)
	}
	first.Tick()
	for _, cpu := range shared.writes[80:] {
		if cpu < 40 || cpu >= 80 {
			t.Fatalf("first shard clobbered foreign control CPU %d", cpu)
		}
	}
	before := len(shared.writes)
	if err := second.Handle(8, eevdf.Event{Kind: eevdf.EvEnqueue, Pid: 8, Cpu: 0, Weight: 100}); err != nil {
		t.Fatal(err)
	}
	second.Tick()
	for _, cpu := range shared.writes[before:] {
		if cpu < 0 || cpu >= 40 {
			t.Fatalf("second shard clobbered foreign control CPU %d", cpu)
		}
	}
}

func cpusFrom(start, n int32) []int32 {
	cpus := make([]int32, n)
	for i := range cpus {
		cpus[i] = start + int32(i)
	}
	return cpus
}
