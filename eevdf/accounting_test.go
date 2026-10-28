package eevdf

import (
	"math"
	"testing"
)

func TestRequestLifecycle(t *testing.T) {
	var r Request
	if !r.Prepare(100, 1000, Nice0Weight, true) || r.Deadline != 600 {
		t.Fatalf("initial request: %+v", r)
	}
	if r.Prepare(200, 1000, Nice0Weight, false) || r.Deadline != 600 {
		t.Fatalf("preemption renewed unfinished request: %+v", r)
	}
	if r.Prepare(200, 1000, 2*Nice0Weight, false) || r.Deadline != 400 {
		t.Fatalf("reweight did not preserve remaining service: %+v", r)
	}
	r.Rebase(200, 1000)
	if r.Deadline != 1200 || r.Slice != 1000 {
		t.Fatalf("rebase: %+v", r)
	}
	if !r.Prepare(1200, 1000, 2*Nice0Weight, false) || r.Deadline != 1700 {
		t.Fatalf("completed request: %+v", r)
	}
	r.Sleep()
	if !r.Prepare(1200, 1000, 2*Nice0Weight, false) {
		t.Fatal("sleep did not discard request")
	}
}

func TestAccountingWrapAndGuards(t *testing.T) {
	if !TimeBefore(math.MaxUint64-2, 1) || TimeBefore(1, math.MaxUint64-2) {
		t.Fatal("wrapped time ordering")
	}
	if got := vdiv(math.MinInt64, 2); got != math.MinInt64/2 {
		t.Fatalf("vdiv minimum: %d", got)
	}
	if vdiv(-5, 2) != -2 || vdiv(7, 0) != 0 {
		t.Fatal("vdiv truncation or zero divisor")
	}
	var r Request
	if !r.Prepare(math.MaxUint64-10, 20, Nice0Weight, false) || r.Deadline != 9 {
		t.Fatalf("wrapped deadline: %+v", r)
	}
	if r.Prepare(math.MaxUint64-5, 20, Nice0Weight, false) {
		t.Fatal("valid deadline at wrapped zero was lost")
	}
	if got := LoadWeight(100); got != Nice0Weight {
		t.Fatalf("weight: %d", got)
	}
	if got := Lag(2, math.MaxUint64-2, 3); got != 3 {
		t.Fatalf("clamped wrap lag: %d", got)
	}
}

func TestWeightedReferenceAndCharges(t *testing.T) {
	const ref = uint64(1000)
	if got := joinDelta(ref, 1024, 800, 1024); int64(got) != -100 {
		t.Fatalf("join delta: %d", got)
	}
	if got := leaveDelta(900, 2048, 800, 1024); got != 100 {
		t.Fatalf("leave delta: %d", got)
	}
	if got := place(ref, 1024, 100, 1024); got != 800 {
		t.Fatalf("lag placement: %d", got)
	}
	a, rem := fold(3, 5, 4, 0)
	b, rem := fold(3, 5, 4, rem)
	if a != 3 || b != 4 || rem != 2 {
		t.Fatalf("charge remainder: %d %d %d", a, b, rem)
	}
	if got := referenceAt(ref, 2048, 1024, 100, 200); got != 1050 {
		t.Fatalf("reference projection: %d", got)
	}
	if got := referenceBeforeJoin(ref, 2048, 1024, 100, 200, 1024); got != 1049 {
		t.Fatalf("join projection: %d", got)
	}
	if got := delayDebt(-100, 60, 1024, 1024); got != -70 {
		t.Fatalf("delayed debt: %d", got)
	}
}

func TestLoadAndGroupHelpers(t *testing.T) {
	if utilEstUpdate(500, 700) != 700 || utilEstUpdate(800, 400) != 700 {
		t.Fatal("util_est update")
	}
	if runnableUpdate(0, 100, 50) != 64 || taskLoad(1024, 1024, 0, UtilHalfLifeNs) != 512 {
		t.Fatal("runnable or asleep load")
	}
	if !wakeWide(20, 4, 4) || wakeWide(8, 1, 4) {
		t.Fatal("wake wide threshold")
	}
	if !wakeAffineWeight(100, 200, -1, 10, false) {
		t.Fatal("wake affine")
	}
	local := ForkGroup{Nr: 2, Allowed: 2, Runnable: 2, Idle: 0, Load: 100, Cap: 1024, Local: true}
	spare := ForkGroup{Nr: 2, Allowed: 2, Runnable: 1, Load: 200, Cap: 1024}
	if !forkGroupWins(spare, &local, true, true) {
		t.Fatal("restricted placement did not prefer spare capacity")
	}
	if !forkGroupWins(local, nil, false, false) {
		t.Fatal("first valid group")
	}
	if got := groupShares(1024, 2, 4, 100, 100, 200, 100); got != 1024 {
		t.Fatalf("group shares: %d", got)
	}
	if got := roundWeight(cgroupLoadWeight(100)); got != 1 {
		t.Fatalf("group weight: %d", got)
	}
	if w, ok := newidleSample(0, 1023); ok || w != 0 {
		t.Fatalf("unexpected sample: %d %t", w, ok)
	}
	if w, ok := newidleSample(1024, 1023); !ok || w != 1 {
		t.Fatalf("missing sample: %d %t", w, ok)
	}
}
