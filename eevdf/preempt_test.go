package eevdf

import "testing"

func TestNonPreemptingWakeShortensProtection(t *testing.T) {
	p := New(&fakeIO{n: make([]uint64, 1)}, 1, Enums{})
	cur := &Task{Pid: 1, Weight: Nice0Weight, CustomSlice: 100,
		Vruntime: 100, Req: Request{Slice: 100, Deadline: 200}}
	wakee := &Task{Pid: 2, Weight: Nice0Weight, CustomSlice: 20,
		Vruntime: 100, Req: Request{Slice: 20, Deadline: 120}}
	p.packs[0].join(cur)
	p.packs[0].join(wakee)
	p.cpus[0].task, p.cpus[0].protect = cur, 200
	p.trees[0].Insert(wakee)
	f := DefaultFeatures()
	f.PreemptShort = false
	if p.shouldPreemptWith(wakee, 0, f) {
		t.Fatal("protected current task was preempted")
	}
	if p.cpus[0].protect != 120 || p.trees[0].Len() != 1 {
		t.Fatalf("protection or queue changed incorrectly: protect=%d len=%d",
			p.cpus[0].protect, p.trees[0].Len())
	}
}

func TestIdleCgroupYieldsToNormalWakee(t *testing.T) {
	p := New(&fakeIO{n: make([]uint64, 1)}, 1, Enums{})
	if err := p.SetCgroup(7, 0, 100); err != nil {
		t.Fatal(err)
	}
	if err := p.SetCgroupIdle(7, true); err != nil {
		t.Fatal(err)
	}
	cur := &Task{Pid: 1, Weight: Nice0Weight, Cgid: 7}
	wakee := &Task{Pid: 2, Weight: Nice0Weight}
	p.cpus[0].task = cur
	if !p.shouldPreemptWith(wakee, 0, DefaultFeatures()) {
		t.Fatal("normal wakee did not preempt idle-group task")
	}
	p.cpus[0].task = wakee
	if p.shouldPreemptWith(cur, 0, DefaultFeatures()) {
		t.Fatal("idle-group wakee preempted normal task")
	}
}
