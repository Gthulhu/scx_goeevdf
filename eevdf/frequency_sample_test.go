package eevdf

import "testing"

func TestFrequencySampleCountsCurrentAndQueue(t *testing.T) {
	io := &fakeIO{n: make([]uint64, 1)}
	p := New(io, 1, Enums{})
	p.Handle(Event{Kind: EvEnqueue, Pid: 1, Cpu: 0, Weight: 100,
		EnqSeq: 1, Now: 1_000_000_000})
	p.Handle(Event{Kind: EvEnqueue, Pid: 2, Cpu: 0, Weight: 100,
		EnqSeq: 1, Now: 1_001_000_000})
	if cpu, ok := p.TaskCPU(2); !ok || cpu != 0 {
		t.Fatal("runnable task must expose selected CPU")
	}
	_, cap, runnable := p.FrequencySample(0, 1_002_000_000)
	if cap != 1024 || runnable != 2 {
		t.Fatalf("capacity=%d runnable=%d, want 1024 and 2", cap, runnable)
	}
	if util, cap, n := p.FrequencySample(-1, 1_002_000_000); util != 0 || cap != 0 || n != 0 {
		t.Fatalf("invalid CPU returned demand %d %d %d", util, cap, n)
	}
	if _, ok := p.TaskCPU(999); ok {
		t.Fatal("unknown task has a CPU")
	}
}
