package eevdf

import "testing"

type publishedIO struct {
	*fakeIO
	pub  map[uint32]TaskPub
	curr int32
}

func (i *publishedIO) Publish(slot uint32, pub TaskPub) { i.pub[slot] = pub }
func (i *publishedIO) CurrPid(int32) int32              { return i.curr }

func TestPolicyPublishesSlotsAndArmsHrtick(t *testing.T) {
	io := &publishedIO{fakeIO: &fakeIO{n: make([]uint64, 1),
		runSeq: []uint64{7}}, pub: make(map[uint32]TaskPub)}
	p := New(io, 1, Enums{})
	p.Handle(Event{Kind: EvEnqueue, Pid: 1, Cpu: 0, Weight: 100,
		EnqSeq: 1, Now: 1_000_000_000})
	first := p.tasks[1].Slot
	if first == 0 || io.pub[first].Pid != 1 || io.cmds[0].Slot != first {
		t.Fatalf("dispatch did not publish its task slot: %+v", io.cmds)
	}

	io.curr = 1
	p.Handle(Event{Kind: EvEnqueue, Pid: 2, Cpu: 0, Weight: 100,
		EnqSeq: 1, Now: 1_000_001_000})
	if len(io.cmds) != 2 || io.cmds[1].Kind != CmdHrtick ||
		io.cmds[1].Slot != 7 || io.pub[p.tasks[2].Slot].Pid != 2 {
		t.Fatalf("first waiter did not arm a task-specific hrtick: %+v", io.cmds)
	}
	p.Handle(Event{Kind: EvExit, Pid: 1, Cpu: 0, Now: 1_000_002_000})
	if io.pub[first].Pid != 0 {
		t.Fatalf("exited task was not unpublished: %+v", io.pub[first])
	}
	p.Handle(Event{Kind: EvEnqueue, Pid: 3, Cpu: 0, Weight: 100,
		EnqSeq: 1, Now: 1_000_003_000})
	if p.tasks[3].Slot != first || io.pub[first].Pid != 3 {
		t.Fatal("released slot was not safely reused")
	}
}

func TestExternallyAssignedShardSlotIsNotRecycledPrivately(t *testing.T) {
	p := New(&fakeIO{n: make([]uint64, 1)}, 1, Enums{})
	tk := &Task{Pid: 20, Slot: 42}
	p.pub.Release(p, tk)
	if tk.Slot != 0 || len(p.pub.free) != 0 {
		t.Fatal("external publication slot entered per-policy free list")
	}
	private := &Task{Pid: 21}
	if p.pub.Allocate(private) != 1 {
		t.Fatalf("private allocator adopted external slot %d", private.Slot)
	}
}
