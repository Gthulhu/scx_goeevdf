package eevdf

import "testing"

func TestMultiLLCNewidlePrefersLocalAndRespectsAffinity(t *testing.T) {
	io := &fakeIO{n: make([]uint64, 4), allowed: map[int32][]int32{
		1: {0}, 2: {0, 2}, 3: {0, 2}, 4: {1, 2}, 5: {1, 2},
		6: {3}, 7: {2, 3},
	}}
	p := New(io, 4, Enums{})
	if err := p.SetTopology(Topology{
		Core: []int{0, 1, 2, 3}, LLC: []int{0, 0, 1, 1},
		Node: []int{0, 0, 0, 0}, Capacity: []uint64{1024, 1024, 1024, 1024},
	}); err != nil {
		t.Fatal(err)
	}
	for _, ev := range []Event{
		{Kind: EvEnqueue, Pid: 1, Cpu: 0, Weight: 100},
		{Kind: EvEnqueue, Pid: 2, Cpu: 0, Weight: 100},
		{Kind: EvEnqueue, Pid: 3, Cpu: 0, Weight: 100},
		{Kind: EvEnqueue, Pid: 4, Cpu: 1, Weight: 100},
		{Kind: EvEnqueue, Pid: 5, Cpu: 1, Weight: 100},
		{Kind: EvEnqueue, Pid: 6, Cpu: 3, Weight: 100},
		{Kind: EvEnqueue, Pid: 7, Cpu: 3, Weight: 100},
	} {
		p.Handle(ev)
	}
	p.pull(2)
	if p.cpus[2].task != p.tasks[7] || p.tasks[1].Cpu != 0 ||
		!p.allowed(p.cpus[2].task, 2) {
		t.Fatalf("failed affinity-safe cross-LLC pull: %+v", p.cpus[2])
	}
}

func TestPeriodicBalanceMovesQueuedTaskAcrossLLC(t *testing.T) {
	io := &fakeIO{n: make([]uint64, 4), allowed: map[int32][]int32{
		1: {0}, 2: {0, 2, 3}, 3: {0, 2, 3},
	}}
	p := New(io, 4, Enums{})
	if err := p.SetTopology(Topology{
		Core: []int{0, 1, 2, 3}, LLC: []int{0, 0, 1, 1},
		Node: []int{0, 0, 0, 0}, Capacity: []uint64{1024, 1024, 1024, 1024},
	}); err != nil {
		t.Fatal(err)
	}
	for id := int32(1); id <= 3; id++ {
		p.Handle(Event{Kind: EvEnqueue, Pid: id, Cpu: 0, Weight: 100})
	}
	p.BalanceTick(100_000_000)
	p.BalanceTick(150_000_000)
	if p.tasks[1].Cpu != 0 || (p.tasks[2].Cpu == 0 && p.tasks[3].Cpu == 0) {
		t.Fatalf("periodic balance failed cross-LLC affinity: CPUs %d %d %d",
			p.tasks[1].Cpu, p.tasks[2].Cpu, p.tasks[3].Cpu)
	}
}

func TestNewidleNodeBeforeSystemAndPinnedDonors(t *testing.T) {
	io := &fakeIO{n: make([]uint64, 5), allowed: map[int32][]int32{
		1: {0}, 2: {0, 4}, 3: {2}, 4: {2, 4},
	}}
	p := New(io, 5, Enums{})
	if err := p.SetTopology(Topology{
		Core: []int{0, 1, 2, 3, 4}, LLC: []int{0, 0, 1, 1, 2},
		Node: []int{0, 0, 0, 0, 1}, Capacity: []uint64{1024, 1024, 1024, 1024, 1024},
	}); err != nil {
		t.Fatal(err)
	}
	for _, ev := range []Event{
		{Kind: EvEnqueue, Pid: 1, Cpu: 0, Weight: 100},
		{Kind: EvEnqueue, Pid: 2, Cpu: 0, Weight: 100},
		{Kind: EvEnqueue, Pid: 3, Cpu: 2, Weight: 100},
		{Kind: EvEnqueue, Pid: 4, Cpu: 2, Weight: 100},
	} {
		p.Handle(ev)
	}
	p.pull(4)
	if p.tasks[1].Cpu != 0 || p.tasks[3].Cpu != 2 || p.tasks[2].Cpu != 4 {
		t.Fatal("cross-node pull did not skip pinned donor and choose allowed queued task")
	}
}
