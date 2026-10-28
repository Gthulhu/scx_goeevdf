package eevdf

import "testing"

func TestEligibilitySwitchChangesPick(t *testing.T) {
	p := New(&fakeIO{n: make([]uint64, 1)}, 1, Enums{})
	p.packs[0].Ref = 50
	early := &Task{Pid: 1, Vruntime: 80, Req: Request{Deadline: 90}}
	eligible := &Task{Pid: 2, Vruntime: 40, Req: Request{Deadline: 120}}
	p.trees[0].Insert(early)
	p.trees[0].Insert(eligible)
	if got := p.pick(0); got != eligible {
		t.Fatal("enabled eligibility picked an ineligible task")
	}
	p.F.EligibleScan = false
	if got := p.pick(0); got != early {
		t.Fatal("disabled eligible scan still scanned past earliest deadline")
	}
	p.F.EligibleScan = true
	p.F.Eligibility = false
	p.F.Normalize()
	if got := p.pick(0); got != early {
		t.Fatal("disabled eligibility still skipped earliest deadline")
	}
}

func TestPlacementAndDelayedDebtSwitches(t *testing.T) {
	p := New(&fakeIO{n: make([]uint64, 2)}, 2, Enums{})
	src, dst := p.packs[0], p.packs[1]
	task := &Task{Pid: 1, Weight: Nice0Weight, Vruntime: 150,
		Req: Request{Deadline: 250, Slice: 100, Weight: Nice0Weight}}
	src.join(task)
	dst.join(&Task{Pid: 2, Weight: Nice0Weight, Vruntime: 100})
	p.F.PlaceLag, p.F.PlaceRelDeadline = false, false
	p.placeTask(task, 1, false)
	if task.Vruntime != 100 || task.Req.Slice != 0 {
		t.Fatalf("disabled lag/relative deadline kept old placement: vr=%d req=%+v",
			task.Vruntime, task.Req)
	}

	task.Vruntime = 200
	dst.quiesceWith(task, true, 100, false)
	if task.delay.pack != nil {
		t.Fatal("disabled delayed dequeue retained sleeping debt")
	}
}

func TestBlockedDonorStaysOnCPUAndRetainsRequest(t *testing.T) {
	const blocked = uint64(2)
	io := &fakeIO{n: make([]uint64, 2), idle: []bool{true, true},
		allowed: map[int32][]int32{1: {0, 1}}}
	p := New(io, 2, Enums{EnqBlocked: blocked})
	p.Handle(Event{Kind: EvEnqueue, Pid: 1, Cpu: 0, Weight: 100,
		EnqSeq: 1})
	task := p.tasks[1]
	task.Req.Slice, task.Req.Deadline = 100_000, task.Vruntime+50_000
	want := task.Req
	io.cmds = nil
	p.Handle(Event{Kind: EvEnqueue, Pid: 1, Cpu: 0, Weight: 100,
		Flags: blocked, EnqSeq: 2})
	if !task.Blocked || task.Cpu != 0 || task.Req != want || task.state != Queued ||
		p.trees[0].Len() != 1 || len(io.cmds) != 0 {
		t.Fatalf("blocked donor migrated, changed service, or shortcut dispatched: %+v %+v", task, io.cmds)
	}
	if got, _ := p.nb.scan(1, []int32{0}, lvLLC, 0, true); got != nil {
		t.Fatal("newidle stole blocked donor")
	}
	if p.trees[0].Len() != 1 {
		t.Fatal("blocked donor removed from original CPU")
	}
	p.F.ProxyExec = false
	p.Handle(Event{Kind: EvEnqueue, Pid: 1, Cpu: 0, Weight: 100,
		Flags: blocked, EnqSeq: 3})
	if task.Blocked {
		t.Fatal("disabled proxy execution marked task blocked")
	}
}

func TestProxyExecRequiresExplicitBlockedEnum(t *testing.T) {
	io := &fakeIO{n: make([]uint64, 1)}
	p := New(io, 1, Enums{})
	p.Handle(Event{Kind: EvEnqueue, Pid: 1, Cpu: 0, Weight: 100,
		Flags: 2, EnqSeq: 1})
	if p.tasks[1].Blocked || len(io.cmds) != 1 {
		t.Fatal("unknown BPF flag must not activate proxy behavior")
	}
}

func TestCgroupAndQuotaSwitches(t *testing.T) {
	io := &fakeIO{n: make([]uint64, 1)}
	p := New(io, 1, Enums{})
	if err := p.SetCgroup(7, 0, 100); err != nil {
		t.Fatal(err)
	}

	if err := p.SetCPUQuota(7, 100_000, 10_000_000); err != nil {
		t.Fatal(err)
	}
	p.F.CpuMax = false
	p.Handle(Event{Kind: EvEnqueue, Pid: 1, Cpu: 0, Weight: 100,
		Cgid: 7, EnqSeq: 1})
	if len(io.cmds) != 1 || io.cmds[0].Slice <= 100_000 ||
		p.throttled(p.tasks[1]) || io.n[0] != 0 {
		t.Fatalf("disabled cpu.max still imposed a quota: %+v", io.cmds)
	}
	p.F.Cgroups = false
	p.F.Normalize()
	p.Handle(Event{Kind: EvEnqueue, Pid: 2, Cpu: 0, Weight: 200,
		Cgid: 7, EnqSeq: 1})
	if p.tasks[2].Weight != p.tasks[2].Nice || p.group(p.tasks[2]) != nil {
		t.Fatal("disabled cgroup weights still affected the task")
	}
}

func TestDisabledDelayDequeueSkipsSleepingDebt(t *testing.T) {
	io := &fakeIO{n: make([]uint64, 1)}
	p := New(io, 1, Enums{})
	p.F.DelayDequeue = false
	p.F.Normalize()
	p.Handle(Event{Kind: EvEnqueue, Pid: 1, Cpu: 0, Weight: 100, EnqSeq: 1})
	p.Handle(Event{Kind: EvEnqueue, Pid: 2, Cpu: 0, Weight: 100, EnqSeq: 1})
	p.tasks[1].Vruntime = p.packs[0].Ref + 100_000
	p.Handle(Event{Kind: EvSleep, Pid: 1})
	if p.tasks[1].lag >= 0 || p.tasks[1].delay.pack != nil {
		t.Fatal("negative lag should remain without delayed-dequeue debt")
	}
}
