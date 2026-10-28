package eevdf

import "testing"

func placementPolicy(n int) (*Policy, *fakeIO) {
	io := &fakeIO{n: make([]uint64, n), idle: make([]bool, n)}
	p := New(io, n, Enums{
		WakeSync: 1, WakeFork: 2, WakeCurrentCPU: 4, WakeTTWU: 8,
	})
	for c := range io.idle {
		io.idle[c] = true
	}
	return p, io
}

func placementTopology(core, llc []int) Topology {
	node, cap := make([]int, len(core)), make([]uint64, len(core))
	for i := range cap {
		cap[i] = Nice0Weight
	}
	return Topology{Core: core, LLC: llc, Node: node, Capacity: cap}
}

func TestWakeWidePrefersPreviousCPUAndRecordsWakerFlips(t *testing.T) {
	p, io := placementPolicy(2)
	if err := p.SetTopology(placementTopology([]int{0, 1}, []int{0, 0})); err != nil {
		t.Fatal(err)
	}
	io.idle[0], io.idle[1] = false, false
	waker := &Task{Pid: 10}
	waker.wake.flips = 4
	p.tasks[waker.Pid] = waker
	p.ld.cpus[0].wakeLoad, p.ld.cpus[1].wakeLoad = 100, 500
	wakee := &Task{Pid: 20, Allowed: []int32{0, 1}}
	wakee.wake.flips = 2
	e := Event{WakeFlags: uint32(p.Enum.WakeTTWU), WakerCpu: 0,
		WakerPid: waker.Pid, Cpu: 1, Pid: wakee.Pid, Now: 1}
	if got := p.wakeAffine(wakee, e, 1); got != 1 {
		t.Fatalf("wide wake selected CPU %d, want previous CPU 1", got)
	}
	if waker.wake.lastWakee != wakee.Pid || waker.wake.flips != 5 {
		t.Fatalf("wakee was not recorded: %+v", waker.wake)
	}

	wakee.wake.flips = 0
	if got := p.wakeAffine(wakee, e, 1); got != 0 {
		t.Fatalf("narrow wake selected CPU %d, want waker CPU 0", got)
	}
}

func TestWakeAffineIdleCPU(t *testing.T) {
	p, io := placementPolicy(2)
	if err := p.SetTopology(placementTopology([]int{0, 1}, []int{0, 0})); err != nil {
		t.Fatal(err)
	}
	io.idle[0], io.idle[1] = true, false
	wakee := &Task{Pid: 20, Allowed: []int32{0, 1}}
	e := Event{WakeFlags: uint32(p.Enum.WakeTTWU), WakerCpu: 0, Cpu: 1}
	if got := p.wakeAffine(wakee, e, 1); got != 0 {
		t.Fatalf("idle waker CPU selected %d, want CPU 0", got)
	}

	io.idle[1] = true
	if got := p.wakeAffine(wakee, e, 1); got != 1 {
		t.Fatalf("both-idle wake selected CPU %d, want previous CPU 1", got)
	}
}

func TestWakeSyncAndWaWeightSwitchesChangePlacement(t *testing.T) {
	t.Run("sync", func(t *testing.T) {
		p, io := placementPolicy(2)
		if err := p.SetTopology(placementTopology([]int{0, 1}, []int{0, 0})); err != nil {
			t.Fatal(err)
		}
		io.idle[0], io.idle[1] = false, false
		waker := &Task{Pid: 10, Weight: Nice0Weight}
		p.tasks[waker.Pid] = waker
		p.cpus[0].task = waker
		p.packs[0].join(waker)
		p.F.WaWeight = false
		wakee := &Task{Pid: 20, Allowed: []int32{0, 1}}
		e := Event{WakeFlags: uint32(p.Enum.WakeTTWU | p.Enum.WakeSync),
			WakerCpu: 0, WakerPid: waker.Pid, Cpu: 1, Now: 1}
		if got := p.selectCPU(wakee, e, true); got != 0 {
			t.Fatalf("synchronous wake selected CPU %d, want waker CPU 0", got)
		}

		p.F.WakeSync = false
		if got := p.selectCPU(wakee, e, true); got != 1 {
			t.Fatalf("disabled WakeSync selected CPU %d, want previous CPU 1", got)
		}
	})

	t.Run("weight", func(t *testing.T) {
		p, io := placementPolicy(2)
		if err := p.SetTopology(placementTopology([]int{0, 1}, []int{0, 0})); err != nil {
			t.Fatal(err)
		}
		io.idle[0], io.idle[1] = false, false
		p.ld.cpus[0].wakeLoad, p.ld.cpus[1].wakeLoad = 100, 500
		wakee := &Task{Pid: 20, Allowed: []int32{0, 1}}
		e := Event{WakeFlags: uint32(p.Enum.WakeTTWU), WakerCpu: 0, Cpu: 1, Now: 1}
		if got := p.selectCPU(wakee, e, true); got != 0 {
			t.Fatalf("weight-aware wake selected CPU %d, want less-loaded CPU 0", got)
		}
		p.F.WaWeight = false
		if got := p.selectCPU(wakee, e, true); got != 1 {
			t.Fatalf("disabled WaWeight selected CPU %d, want previous CPU 1", got)
		}
	})
}

func TestForkPlacementUsesIdlestAllowedLLC(t *testing.T) {
	p, _ := placementPolicy(3)
	if err := p.SetTopology(placementTopology(
		[]int{0, 1, 2}, []int{0, 0, 1})); err != nil {
		t.Fatal(err)
	}
	running := &Task{Pid: 10, Weight: Nice0Weight}
	p.cpus[0].task = running
	p.packs[0].join(running)
	wakee := &Task{Pid: 20, Allowed: []int32{0, 1, 2}}
	e := Event{WakeFlags: uint32(p.Enum.WakeFork), WakerCpu: 0, Cpu: 0}
	if got := p.selectCPU(wakee, e, false); got != 2 {
		t.Fatalf("fork selected CPU %d, want idle CPU 2 in the least-loaded LLC", got)
	}
	wakee.Allowed = []int32{1}
	if got := p.selectCPU(wakee, e, false); got != 1 {
		t.Fatalf("fork escaped affinity mask to CPU %d", got)
	}
}

func TestForkPlacementPrefersFittingAsymmetricCapacity(t *testing.T) {
	p, _ := placementPolicy(2)
	topo := placementTopology([]int{0, 1}, []int{0, 0})
	topo.Capacity = []uint64{512, 1024}
	if err := p.SetTopology(topo); err != nil {
		t.Fatal(err)
	}
	wakee := &Task{Pid: 20, Allowed: []int32{0, 1}}
	p.ld.state(wakee).utilEst = 800
	e := Event{WakeFlags: uint32(p.Enum.WakeFork), WakerCpu: 0, Cpu: 0}
	if got := p.selectCPU(wakee, e, false); got != 1 {
		t.Fatalf("asymmetric fork selected CPU %d, want fitting-capacity CPU 1", got)
	}
}

func TestPlacementPreservesDebtAndHonorsWakeFlags(t *testing.T) {
	p, _ := placementPolicy(2)
	if err := p.SetTopology(placementTopology([]int{0, 1}, []int{0, 0})); err != nil {
		t.Fatal(err)
	}
	wakee := &Task{Pid: 20, Allowed: []int32{0, 1}, lag: -100}
	wakee.delay = delayRec{pack: p.packs[0], gen: p.packs[0].emptyGen}
	e := Event{WakeFlags: uint32(p.Enum.WakeFork | p.Enum.WakeTTWU),
		WakerCpu: 1, Cpu: 1, Now: 1}
	if got := p.selectCPU(wakee, e, true); got != 0 {
		t.Fatalf("negative-lag task moved to CPU %d, want debt pack CPU 0", got)
	}

	wakee.delay = delayRec{}
	e.WakeFlags = uint32(p.Enum.WakeCurrentCPU)
	if got := p.selectCPU(wakee, e, true); got != 1 {
		t.Fatalf("WakeCurrentCPU selected CPU %d, want CPU 1", got)
	}
	e.WakeFlags = uint32(p.Enum.WakeFork)
	wakee.Blocked = true
	if got := p.selectCPU(wakee, e, false); got != 1 {
		t.Fatalf("blocked task moved to CPU %d, want original CPU 1", got)
	}
}
