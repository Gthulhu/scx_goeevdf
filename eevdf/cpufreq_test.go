package eevdf

import (
	"math"
	"testing"
)

func TestPerfTarget(t *testing.T) {
	for _, tt := range []struct {
		name                            string
		utilization, capacity, runnable uint64
		want                            uint64
	}{
		{"idle minimum", 0, 1024, 0, 1},
		{"light load", 256, 1024, 1, 320},
		{"headroom", 512, 1024, 1, 640},
		{"saturation", 819, 1024, 1, 1023},
		{"full load", 820, 1024, 1, 1024},
		{"queue saturation", 100, 1024, 2, 1024},
		{"scaled capacity", 256, 512, 1, 640},
		{"capacity exceeded", 600, 512, 1, 1024},
		{"zero capacity fallback", 512, 0, 1, 640},
		{"invalid large inputs", math.MaxUint64, math.MaxUint64, 1, 1024},
	} {
		t.Run(tt.name, func(t *testing.T) {
			got := PerfTarget(tt.utilization, tt.capacity, tt.runnable)
			if got != tt.want {
				t.Fatalf("PerfTarget(%d, %d, %d) = %d, want %d",
					tt.utilization, tt.capacity, tt.runnable, got, tt.want)
			}
		})
	}
}

func TestFrequencyGovernorTransitionsAndRateLimit(t *testing.T) {
	g := NewFrequencyGovernor(2, 5_000_000)
	check := func(cpu int32, now, util, capacity, runnable uint64, attach bool, want uint64, changed bool) {
		t.Helper()
		got, didChange := g.Update(cpu, now, util, capacity, runnable, attach)
		if got != want || didChange != changed {
			t.Fatalf("Update(cpu=%d, now=%d, util=%d, runnable=%d, attach=%t) = (%d, %t), want (%d, %t)",
				cpu, now, util, runnable, attach, got, didChange, want, changed)
		}
	}
	check(0, 0, 0, 1024, 0, false, 1, true)
	check(0, 1_000_000, 256, 1024, 1, false, 1, false)
	check(0, 1_000_000, 0, 1024, 1, true, 1024, true)
	check(0, 2_000_000, 512, 1024, 1, false, 1024, false)
	check(0, 6_000_000, 512, 1024, 1, false, 640, true)
	check(0, 7_000_000, 0, 1024, 0, false, 640, false)
	check(0, 11_000_000, 0, 1024, 0, false, 1, true)
	check(0, 12_000_000, 256, 1024, 1, false, 1, false)
	check(0, 12_000_001, 256, 1024, 1, true, 1024, true)
	check(0, 17_000_001, 100, 1024, 2, false, 1024, false)
	check(1, 1, 512, 1024, 1, false, 640, true)
	if g.Target(0) != 1024 || g.Target(1) != 640 {
		t.Fatalf("per-CPU targets: %d, %d", g.Target(0), g.Target(1))
	}
	check(1, 2, 100, 1024, 2, false, 640, false)
	check(1, 5_000_001, 100, 1024, 2, false, 1024, true)
}

func TestFrequencyGovernorClockResetAndInvalidCPU(t *testing.T) {
	g := NewFrequencyGovernor(1, 5)
	if target, changed := g.Update(0, 100, 512, 1024, 1, false); target != 640 || !changed {
		t.Fatalf("initial update: %d, %t", target, changed)
	}
	if target, changed := g.Update(0, 1, 256, 1024, 1, false); target != 320 || !changed {
		t.Fatalf("clock reset: %d, %t", target, changed)
	}
	if target, changed := g.Update(0, 5, 0, 1024, 0, false); target != 320 || changed {
		t.Fatalf("rate limit after reset: %d, %t", target, changed)
	}
	if target, changed := g.Update(0, 6, 0, 1024, 0, false); target != 1 || !changed {
		t.Fatalf("elapsed rate limit: %d, %t", target, changed)
	}
	for _, cpu := range []int32{-1, 1} {
		if target, changed := g.Update(cpu, 100, 1024, 1024, 1, false); target != 0 || changed || g.Target(cpu) != 0 {
			t.Fatalf("invalid CPU %d: %d, %t", cpu, target, changed)
		}
	}
}
