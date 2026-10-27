package eevdf

import (
	"math"
	"testing"
)

func TestPressureRTAndStat(t *testing.T) {
	pr := NewPressure(2)
	if pr.Update(0, 100, true, 10_000, StatSample{IRQ: 2_000}) {
		t.Fatal("first sample completed a window")
	}
	if got := pr.Available(0); got != 1024 {
		t.Fatalf("before first window: %d", got)
	}
	if pr.Update(0, 16_000_100, true, 8_010_000, StatSample{IRQ: 2_000}) {
		t.Fatal("short window evaluated")
	}
	if !pr.Update(0, 32_000_100, true, 16_010_000, StatSample{IRQ: 2_000}) {
		t.Fatal("32ms window not evaluated")
	}
	if got := pr.Available(0); got != 512 {
		t.Fatalf("50%% RT occupancy: got %d, want 512", got)
	}
	if got := pr.BalanceCapacity(0, 768); got != 384 {
		t.Fatalf("balance capacity: %d", got)
	}
	if got := pr.Available(1); got != 1024 {
		t.Fatalf("unsampled CPU: %d", got)
	}

	// IRQ, softirq and steal are counted separately from displaced time.
	pr.Update(0, 64_000_100, true, 16_010_000,
		StatSample{IRQ: 8_002_000, SoftIRQ: 4_000_000, Steal: 4_000_000})
	if got := pr.Available(0); got != 512 {
		t.Fatalf("50%% proc/stat loss: got %d, want 512", got)
	}
	pr.Update(0, 96_000_100, true, 16_010_000,
		StatSample{IRQ: 8_002_000, SoftIRQ: 4_000_000, Steal: 4_000_000})
	if got := pr.Available(0); got != 640 {
		t.Fatalf("3:1 smoothing: got %d, want 640", got)
	}
}

func TestPressureResetSaturationAndIdle(t *testing.T) {
	pr := NewPressure(1)
	pr.Update(0, 0, true, math.MaxUint64-10, StatSample{IRQ: math.MaxUint64 - 2})
	pr.Update(0, pressureEvalNs, true, math.MaxUint64, StatSample{IRQ: math.MaxUint64})
	if got := pr.Available(0); got != 1024 {
		t.Fatalf("near wrap: got %d", got)
	}
	// A counter reset must not be interpreted as a huge unsigned delta.
	pr.Update(0, 2*pressureEvalNs, true, 1, StatSample{IRQ: 1})
	if got := pr.Available(0); got != 1024 {
		t.Fatalf("counter reset: got %d", got)
	}
	pr.Update(0, 3*pressureEvalNs, true, 1+pressureEvalNs*2, StatSample{IRQ: 1})
	if got := pr.Available(0); got != 768 {
		t.Fatalf("saturated 100%% lost window: got %d", got)
	}
	pr.Update(0, 3*pressureEvalNs+1, false, 0, StatSample{})
	if got := pr.Available(0); got != 1024 {
		t.Fatalf("idle CPU should have full capacity: %d", got)
	}
	pr.Update(0, 3*pressureEvalNs+pressureIdleDropNs+2, true, 4, StatSample{})
	pr.Update(0, 4*pressureEvalNs+pressureIdleDropNs+2, true, 4, StatSample{})
	if got := pr.Available(0); got != 1024 {
		t.Fatalf("stale pressure persisted after idle: %d", got)
	}
	// Clock reset rebases rather than measuring an enormous interval.
	pr.Update(0, 1, true, 4, StatSample{})
	if pr.Update(0, 2, true, 4, StatSample{}) {
		t.Fatal("clock reset completed a window")
	}
}

func TestParseProcStat(t *testing.T) {
	data := []byte("cpu  1 2 3 4 5 6 7 8 9 10\n" +
		"cpu0  10 20 30 40 50 6 7 8 9 10\n" +
		"cpu2\t10\t20\t30\t40\t50\t12\t14\t16\n" +
		"cpu99 0 0 0 0 0 1 2 3\nintr 1 2\n")
	samples := []StatSample{{}, {IRQ: 123}, {}}
	if err := ParseProcStat(data, samples, 100); err != nil {
		t.Fatal(err)
	}
	if want := (StatSample{60_000_000, 70_000_000, 80_000_000}); samples[0] != want {
		t.Fatalf("CPU 0: %+v, want %+v", samples[0], want)
	}
	if samples[1].IRQ != 123 {
		t.Fatalf("missing CPU overwritten: %+v", samples[1])
	}
	if want := (StatSample{120_000_000, 140_000_000, 160_000_000}); samples[2] != want {
		t.Fatalf("CPU 2: %+v, want %+v", samples[2], want)
	}
	for _, input := range []string{
		"cpu0 1 2 3 4 5 6 7\n", "cpu0 1 2 3 4 5 6 x 8\n",
		"cpu0 1 2 3 4 5 6 7 18446744073709551616\n",
	} {
		if err := ParseProcStat([]byte(input), samples, 100); err == nil {
			t.Fatalf("accepted malformed line %q", input)
		}
	}
	if err := ParseProcStat(data, samples, 0); err == nil {
		t.Fatal("accepted zero USER_HZ")
	}
}

func TestPressureNoAllocations(t *testing.T) {
	pr := NewPressure(1)
	buf := []byte("cpu0 1 2 3 4 5 6 7 8\n")
	var samples [1]StatSample
	if n := testing.AllocsPerRun(100, func() {
		if err := ParseProcStat(buf, samples[:], 100); err != nil {
			panic(err)
		}
		pr.Update(0, 64_000_000, true, 0, samples[0])
		pr.Available(0)
	}); n != 0 {
		t.Fatalf("sampling allocated %g times", n)
	}
}
