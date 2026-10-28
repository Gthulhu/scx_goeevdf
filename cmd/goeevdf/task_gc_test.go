package main

import (
	"errors"
	"syscall"
	"testing"
	"time"

	"github.com/Gthulhu/scx_goeevdf/internal/bpf"
)

func TestTaskGCRecoversLostExit(t *testing.T) {
	var g taskGC
	g.observe(bpf.Ev{Kind: bpf.EvEnqueue, Pid: 42, Cpu: 3, Slot: 9, Now: 1, SumExec: 100})
	g.observe(bpf.Ev{Kind: bpf.EvSleep, Pid: 42, Cpu: 3, Slot: 9, Now: 2, SumExec: 200})
	var exits []bpf.Ev
	err := g.collect(uint64(3*time.Second), 1,
		func(int32) error { return syscall.ESRCH },
		func(ev bpf.Ev) error { exits = append(exits, ev); return nil })
	if err != nil || len(exits) != 1 || exits[0].Kind != bpf.EvExit ||
		exits[0].SumExec != 200 || exits[0].Slot != 9 || len(g.tasks) != 0 {
		t.Fatalf("GC exits = %+v, error %v", exits, err)
	}
}

func TestTaskGCIsBoundedAndChecksErrors(t *testing.T) {
	var g taskGC
	for pid := int32(1); pid <= 100; pid++ {
		g.observe(bpf.Ev{Kind: bpf.EvEnqueue, Pid: pid, Now: 1})
	}

	checked := 0
	emit := func(bpf.Ev) error { t.Fatal("live task collected"); return nil }
	if err := g.collect(uint64(3*time.Second), 0, func(int32) error {
		checked++
		return syscall.EPERM
	}, emit); err != nil || checked == 0 || checked > 64 {
		t.Fatalf("checks = %d, error %v", checked, err)
	}
	err := g.collect(uint64(4*time.Second), 0,
		func(int32) error { return syscall.EIO }, emit)
	if !errors.Is(err, syscall.EIO) {
		t.Fatalf("GC hid probe failure: %v", err)
	}
	g.observe(bpf.Ev{Kind: bpf.EvExit, Pid: 1})
	if len(g.tasks) != 99 {
		t.Fatal("real exit did not retire GC entry")
	}
}

func TestTaskGCResumesBoundedSweep(t *testing.T) {
	var g taskGC
	for pid := int32(1); pid <= 150; pid++ {
		g.observe(bpf.Ev{Kind: bpf.EvEnqueue, Pid: pid, Now: 1})
	}
	seen := make(map[int32]bool)
	now := uint64(3 * time.Second)
	for attempts := 0; len(seen) < 150 && attempts < 100; attempts++ {
		err := g.collect(now, 0, func(pid int32) error {
			seen[pid] = true
			return nil
		}, func(bpf.Ev) error { t.Fatal("live PID collected"); return nil })
		if err != nil {
			t.Fatal(err)
		}
	}
	if len(seen) != 150 || g.remaining != 0 || g.next <= now {
		t.Fatalf("sweep did not resume: seen=%d remaining=%d next=%d", len(seen), g.remaining, g.next)
	}
}
