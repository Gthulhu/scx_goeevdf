package eevdf

import "testing"

func TestActiveSMTMigrationHandoff(t *testing.T) {
	io := &fakeIO{n: make([]uint64, 4), allowed: map[int32][]int32{
		1: {0, 2}, 2: {1},
	}}
	p := New(io, 4, Enums{})
	if err := p.SetTopology(Topology{
		Core: []int{0, 0, 1, 1}, LLC: []int{0, 0, 0, 0},
		Node: []int{0, 0, 0, 0}, Capacity: []uint64{1024, 1024, 1024, 1024},
	}); err != nil {
		t.Fatal(err)
	}
	p.Handle(Event{Kind: EvEnqueue, Pid: 1, Cpu: 0, Weight: 100, EnqSeq: 1})
	p.Handle(Event{Kind: EvEnqueue, Pid: 2, Cpu: 1, Weight: 100, EnqSeq: 1})
	p.BalanceTick(10_000_000)
	if _, ok := p.act.migrate[p.tasks[1]]; !ok ||
		io.cmds[len(io.cmds)-1].Kind != CmdStop {
		t.Fatalf("failed to request active SMT balance: %+v", io.cmds)
	}
	p.Handle(Event{Kind: EvEnqueue, Pid: 1, Cpu: 0, Weight: 100, EnqSeq: 2, Now: 10_100_000})
	if p.tasks[1].Cpu != 2 || p.act.cpus[2].reserved {
		t.Fatal("active handoff did not transfer to idle core")
	}
}
