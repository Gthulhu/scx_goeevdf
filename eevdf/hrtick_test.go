package eevdf

import "testing"

func TestHrtickJoinedLoneAndRunSeq(t *testing.T) {
	io := &fakeIO{n: make([]uint64, 1), runSeq: []uint64{1<<32 | 13}}
	p := New(io, 1, Enums{})
	cur := &Task{Pid: 1, Weight: Nice0Weight, Vruntime: 20,
		Req: Request{Deadline: 100}}
	p.cpus[0].task = cur
	wait := &Task{Pid: 2, Vruntime: 20, Req: Request{Deadline: 110}}
	p.trees[0].Insert(wait)
	h := NewHrtick(p)
	h.joinedLone(0, 1000)
	if len(io.cmds) != 0 { // less than 10 us: a normal tick handles this
		t.Fatal("armed too-short timer")
	}
	cur.Req.Deadline = 40_020
	h.joinedLone(0, 1000)
	if len(io.cmds) != 1 || io.cmds[0].Kind != CmdHrtick ||
		io.cmds[0].Slice != 41_000 || io.cmds[0].Slot != 13 {
		t.Fatal("absolute timer or run sequence mismatch", io.cmds)
	}
	h.joinedLone(0, 1000)
	if len(io.cmds) != 1 {
		t.Fatal("later timer replaced earlier one")
	}
	h.dispatched(0, 2000, 10_000, CmdfHrtick)
	if h.at[0] != 12_000 || h.dispatchFlag(0) != CmdfHrtick {
		t.Fatal("dispatch did not set pending hrtick")
	}
	h.Enabled = false
	if h.dispatchFlag(0) != 0 {
		t.Fatal("disabled timer flag")
	}
}

func TestHrtickNoRunSeqAndFailure(t *testing.T) {
	io := &fakeIO{n: make([]uint64, 1), fail: true}
	p := New(io, 1, Enums{})
	p.cpus[0].task = &Task{Weight: Nice0Weight, Req: Request{Deadline: 50_000}}
	p.trees[0].Insert(&Task{Req: Request{Deadline: 60_000}})
	h := NewHrtick(p)
	h.start(0, 10)
	if h.at[0] != 0 {
		t.Fatal("failed timer submit was remembered")
	}
}
