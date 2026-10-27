package eevdf

import "testing"

func TestCPUQuotaParksAndRefillsFIFO(t *testing.T) {
	io := &fakeIO{n: make([]uint64, 1)}
	p := New(io, 1, Enums{})
	if err := p.SetCgroup(1, 0, 100); err != nil {
		t.Fatal(err)
	}
	if err := p.SetCPUQuota(1, 100_000, 1_000_000); err != nil {
		t.Fatal(err)
	}
	p.Periodic(10_000_000)
	for pid := int32(1); pid <= 2; pid++ {
		p.Handle(Event{Kind: EvEnqueue, Pid: pid, Cpu: 0, Cgid: 1, Weight: 100, EnqSeq: 1})
	}
	if len(io.cmds) != 1 || io.cmds[0].Slice != 100_000 || p.bandwidth[0].pool != 0 {
		t.Fatalf("dispatch did not reserve quota: %+v", io.cmds)
	}
	p.Handle(Event{Kind: EvEnqueue, Pid: 1, Cpu: 0, Cgid: 1,
		Weight: 100, SumExec: 100_000, EnqSeq: 2})
	p.Schedule()
	if !p.HasBacklog() {
		t.Fatal("throttled runnable backlog must keep scheduler awake")
	}
	if !p.Pending() {
		t.Fatal("quota backlog is pending work")
	}
	if p.tasks[1].state != Parked || p.tasks[2].state != Parked ||
		p.packs[0].W != 0 || io.n[0] != 0 || len(io.cmds) != 1 {
		t.Fatalf("quota failed to park runnable tasks: states %v %v, cmds %+v",
			p.tasks[1].state, p.tasks[2].state, io.cmds)
	}
	p.Periodic(10_999_999)
	if len(io.cmds) != 1 {
		t.Fatal("refilled before period")
	}
	p.Periodic(11_000_000)
	if p.HasBacklog() {
		t.Fatal("refilled backlog should no longer keep scheduler awake")
	}
	if !p.Pending() {
		t.Fatal("refilled task still queued in EDQ is pending work")
	}
	if len(io.cmds) != 2 || io.cmds[1].Pid != 2 ||
		p.tasks[1].state != Queued || io.n[0] != 1 {
		t.Fatalf("FIFO refill did not restore runnable tasks: %+v", io.cmds)
	}
	p.Handle(Event{Kind: EvExit, Pid: 2})
	if len(p.bandwidth[0].backlog) != 0 || p.tasks[2] != nil {
		t.Fatal("exited task remains in backlog")
	}
}

func TestAncestorQuotaAndMulticpuReservation(t *testing.T) {
	io := &fakeIO{n: make([]uint64, 2)}
	p := New(io, 2, Enums{})
	for _, cfg := range [][3]uint64{{1, 0, 100}, {2, 1, 100}} {
		if err := p.SetCgroup(cfg[0], cfg[1], cfg[2]); err != nil {
			t.Fatal(err)
		}
	}
	if err := p.SetCPUQuota(1, 150_000, 1_000_000); err != nil {
		t.Fatal(err)
	}
	p.Handle(Event{Kind: EvEnqueue, Pid: 1, Cpu: 0, Cgid: 2, Weight: 100})
	p.Handle(Event{Kind: EvEnqueue, Pid: 2, Cpu: 1, Cgid: 2, Weight: 100})
	if len(io.cmds) != 1 || io.cmds[0].Slice != 150_000 ||
		p.tasks[2].state != Parked {
		t.Fatalf("second CPU overcommitted parent quota: %+v", io.cmds)
	}
	p.Periodic(2_000_000)
	p.Periodic(3_000_000)
	if len(io.cmds) != 2 || io.cmds[1].Pid != 2 {
		t.Fatalf("ancestor quota did not refill descendant: %+v", io.cmds)
	}
}

func TestQuotaDisableRestoresParkedTask(t *testing.T) {
	io := &fakeIO{n: make([]uint64, 1)}
	p := New(io, 1, Enums{})
	if err := p.SetCgroup(1, 0, 100); err != nil {
		t.Fatal(err)
	}

	if err := p.SetCPUQuota(1, 10_000, 1_000_000); err != nil {
		t.Fatal(err)
	}

	p.Handle(Event{Kind: EvEnqueue, Pid: 1, Cpu: 0, Cgid: 1, Weight: 100})
	p.Handle(Event{Kind: EvEnqueue, Pid: 1, Cpu: 0, Cgid: 1,
		Weight: 100, SumExec: 10_000, EnqSeq: 2})
	if p.tasks[1].state != Parked {
		t.Fatal("task should be parked")
	}
	if err := p.SetCPUQuota(1, 0, 0); err != nil {
		t.Fatal(err)
	}
	if p.HasBacklog() {
		t.Fatal("disabling quota must release runnable backlog")
	}
	if p.tasks[1].state != Dispatched || len(io.cmds) != 2 {
		t.Fatal("removing cpu.max did not wake parked task")
	}
}

func TestQuotaExitRemovesParkedTask(t *testing.T) {
	io := &fakeIO{n: make([]uint64, 1)}
	p := New(io, 1, Enums{})
	if err := p.SetCgroup(1, 0, 100); err != nil {
		t.Fatal(err)
	}

	if err := p.SetCPUQuota(1, 10_000, 1_000_000); err != nil {
		t.Fatal(err)
	}
	p.Handle(Event{Kind: EvEnqueue, Pid: 1, Cpu: 0, Cgid: 1, Weight: 100})
	p.Handle(Event{Kind: EvEnqueue, Pid: 1, Cpu: 0, Cgid: 1,
		Weight: 100, SumExec: 10_000, EnqSeq: 2})
	p.Handle(Event{Kind: EvExit, Pid: 1, SumExec: 10_000})
	p.Periodic(1_000_000)
	p.Periodic(2_000_000)
	if len(io.cmds) != 1 || len(p.bandwidth[0].backlog) != 0 ||
		p.tasks[1] != nil {
		t.Fatal("exited task was dispatched again")
	}
}

func TestPendingIncludesFailedDispatchRetry(t *testing.T) {
	io := &fakeIO{n: make([]uint64, 1), fail: true}
	p := New(io, 1, Enums{})
	if p.Pending() {
		t.Fatal("new policy has no pending tasks")
	}
	p.Handle(Event{Kind: EvEnqueue, Pid: 1, Cpu: 0, Weight: 100})
	if !p.Pending() || p.HasBacklog() {
		t.Fatal("failed dispatch must remain pending in EDQ")
	}
	io.fail = false
	p.Schedule()
	if p.Pending() {
		t.Fatal("successful dispatch drained pending work")
	}
}
