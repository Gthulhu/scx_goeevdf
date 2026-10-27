package eevdf

import "testing"

func TestFeatureDefaultsAndDependencies(t *testing.T) {
	f := DefaultFeatures()
	if !f.Hrtick || !f.Eligibility || !f.ProxyExec || !f.Cgroups || f.LLCExtend || f.SMTWholeCore {
		t.Fatal("unexpected feature defaults", f)
	}
	f.Eligibility, f.DelayDequeue, f.Cgroups = false, false, false
	f.Normalize()
	if f.EligibleScan || f.RunToParity || f.DelayRequeue || f.CpuMax {
		t.Fatal("disabled parent still has active dependent feature", f)
	}
}

func TestWakeupPreemptFeature(t *testing.T) {
	io := &fakeIO{n: make([]uint64, 1)}
	p := New(io, 1, Enums{})
	cur := &Task{Pid: 1, Weight: Nice0Weight, Req: Request{Slice: 100, Deadline: 200}}
	w := &Task{Pid: 2, Weight: Nice0Weight, Req: Request{Slice: 10, Deadline: 100}}
	p.cpus[0].task = cur
	p.trees[0].Insert(w)
	f := DefaultFeatures()
	if !p.shouldPreemptWith(w, 0, f) {
		t.Fatal("short wakeup did not preempt")
	}
	f.WakeupPreempt = false
	if p.shouldPreemptWith(w, 0, f) {
		t.Fatal("disabled wakeup preemption still preempted")
	}
}
