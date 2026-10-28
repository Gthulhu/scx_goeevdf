package eevdf

import "testing"

func TestLoadUtilEstimateAndDecay(t *testing.T) {
	p := New(&fakeIO{n: make([]uint64, 1)}, 1, Enums{})
	task := &Task{Weight: Nice0Weight}
	p.ld.taskRunning(task, true, 1)
	p.ld.taskQuiesce(task, true, 32_000_001)
	s := p.ld.state(task)
	if s.utilEst < 500 || s.utilEst > 520 {
		t.Fatalf("util estimate %d", s.utilEst)
	}
	if p.ld.taskLoad(task, 96_000_001) >= p.ld.taskLoad(task, 32_000_001) {
		t.Fatal("sleeping task load did not decay")
	}
	p.ld.taskWoke(task, 128_000_001)
	if p.ld.taskUtil(task, 128_000_001) < s.utilEst {
		t.Fatal("wakeup lost util estimate")
	}
}
