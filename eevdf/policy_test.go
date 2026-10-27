package eevdf

import (
	"errors"
	"testing"
)

type fakeIO struct {
	cmds    []Cmd
	fail    bool
	n       []uint64
	idle    []bool
	allowed map[int32][]int32
}

func (f *fakeIO) Submit(c Cmd) error {
	if f.fail {
		return errors.New("ring full")
	}
	f.cmds = append(f.cmds, c)
	return nil
}
func (f *fakeIO) Idle(c int32) bool {
	if f.idle == nil {
		return true
	}
	return f.idle[c]
}
func (*fakeIO) CurrPid(int32) int32                   { return 0 }
func (f *fakeIO) SetQueued(c int32, n, _ uint64)      { f.n[c] = n }
func (f *fakeIO) Affinity(pid int32) ([]int32, error) { return f.allowed[pid], nil }

func TestEEVDFPicksEligibleBeforeDeadline(t *testing.T) {
	io := &fakeIO{n: make([]uint64, 1)}
	p := New(io, 1, Enums{EnqWakeup: 1})
	p.Handle(Event{Kind: EvEnqueue, Pid: 1, Cpu: 0, Weight: 100, EnqSeq: 1})
	if len(io.cmds) != 1 || io.cmds[0].Pid != 1 {
		t.Fatal("first task must dispatch", io.cmds)
	}
	p.Handle(Event{Kind: EvEnqueue, Pid: 2, Cpu: 0, Weight: 100, EnqSeq: 1})
	p.Handle(Event{Kind: EvEnqueue, Pid: 3, Cpu: 0, Weight: 100, EnqSeq: 1})
	if io.n[0] != 2 {
		t.Fatalf("queued=%d, want 2", io.n[0])
	}
	// A slice expiration charges the running task before picking the next one.
	p.Handle(Event{Kind: EvEnqueue, Pid: 1, Cpu: 0, Weight: 100, SumExec: p.Slice, EnqSeq: 2})
	p.Schedule()
	if got := io.cmds[len(io.cmds)-1].Pid; got != 2 {
		t.Fatalf("picked %d, want eligible earliest deadline task 2", got)
	}
	if p.packs[0].W != 3*Nice0Weight {
		t.Fatalf("pack weight %d", p.packs[0].W)
	}
}

func TestFailedSubmitRetainsTask(t *testing.T) {
	io := &fakeIO{n: make([]uint64, 1), fail: true}
	p := New(io, 1, Enums{})
	p.Handle(Event{Kind: EvEnqueue, Pid: 4, Cpu: 0, Weight: 100, EnqSeq: 3})
	if p.trees[0].Len() != 1 || io.n[0] != 1 {
		t.Fatal("failed dispatch lost task")
	}
	io.fail = false
	p.Schedule()
	if p.trees[0].Len() != 0 || len(io.cmds) != 1 || io.cmds[0].EnqSeq != 3 {
		t.Fatal("retry did not dispatch original enqueue", io.cmds)
	}
}

func TestSleepLagAndExit(t *testing.T) {
	io := &fakeIO{n: make([]uint64, 1)}
	p := New(io, 1, Enums{EnqWakeup: 1})
	p.Handle(Event{Kind: EvEnqueue, Pid: 1, Cpu: 0, Weight: 100, EnqSeq: 1})
	p.Handle(Event{Kind: EvEnqueue, Pid: 2, Cpu: 0, Weight: 100, EnqSeq: 1})
	p.Handle(Event{Kind: EvSleep, Pid: 1, SumExec: p.Slice})
	if p.packs[0].W != Nice0Weight || p.tasks[1].pack != nil {
		t.Fatal("sleep did not remove task from pack")
	}
	p.Handle(Event{Kind: EvEnqueue, Pid: 1, Cpu: 0, Weight: 100, Flags: 1, EnqSeq: 2, SumExec: p.Slice})
	if p.tasks[1].pack != p.packs[0] {
		t.Fatal("wake did not rejoin pack")
	}
	p.Handle(Event{Kind: EvExit, Pid: 1, SumExec: p.Slice})
	if p.tasks[1] != nil || p.packs[0].W != Nice0Weight {
		t.Fatal("exit leaked task")
	}
}

func TestIdlePlacementAndPullRespectAffinity(t *testing.T) {
	io := &fakeIO{
		n: make([]uint64, 3), idle: []bool{true, true, false},
		allowed: map[int32][]int32{
			1: {0}, 2: {0, 1}, 3: {0},
		},
	}

	p := New(io, 3, Enums{EnqWakeup: 1})
	p.Handle(Event{Kind: EvEnqueue, Pid: 1, Cpu: 0, Weight: 100, EnqSeq: 1})
	io.idle[0] = false
	p.Handle(Event{Kind: EvEnqueue, Pid: 2, Cpu: 0, Weight: 100, Flags: 1, EnqSeq: 1})
	if io.cmds[len(io.cmds)-1].Cpu != 1 {
		t.Fatal("woken task should use allowed vacant CPU", io.cmds)
	}
	io.idle[1] = false
	p.Handle(Event{Kind: EvEnqueue, Pid: 3, Cpu: 0, Weight: 100, EnqSeq: 1})
	if p.trees[0].Len() != 1 {
		t.Fatal("pinned task not queued on allowed CPU")
	}
	p.Handle(Event{Kind: EvIdle, Cpu: 1})
	p.Schedule()
	if p.tasks[3].Cpu != 0 {
		t.Fatal("idle pull illegally migrated pinned task")
	}
	io.allowed[3] = []int32{0, 1}
	p.Handle(Event{Kind: EvCpumask, Pid: 3})
	p.Handle(Event{Kind: EvSleep, Pid: 2})
	io.idle[1] = true
	p.Handle(Event{Kind: EvIdle, Cpu: 1})
	p.Schedule()
	if p.tasks[3].Cpu != 1 || p.trees[0].Len() != 0 {
		t.Fatal("idle CPU failed to pull newly allowed queued task")
	}
}

func TestWeightedCPUShare(t *testing.T) {
	io := &fakeIO{n: make([]uint64, 1)}
	p := New(io, 1, Enums{})
	p.Handle(Event{Kind: EvEnqueue, Pid: 1, Cpu: 0, Weight: 100, EnqSeq: 1})
	p.Handle(Event{Kind: EvEnqueue, Pid: 2, Cpu: 0, Weight: 200, EnqSeq: 1})
	var runtime [3]uint64
	var sequence [3]uint64
	sequence[1], sequence[2] = 1, 1
	for i := 0; i < 900; i++ {
		pid := io.cmds[len(io.cmds)-1].Pid
		runtime[pid] += p.Slice
		sequence[pid]++
		p.Handle(Event{Kind: EvEnqueue, Pid: pid, Cpu: 0,
			Weight: uint64(pid) * 100, SumExec: runtime[pid], EnqSeq: sequence[pid]})
		p.Schedule()
	}

	ratio := float64(runtime[2]) / float64(runtime[1])
	if ratio < 1.8 || ratio > 2.2 {
		t.Fatalf("weighted share ratio %.2f (want ~2), runtimes %v", ratio, runtime)
	}
}

func TestIdlePullPrefersEligibleDeadline(t *testing.T) {
	io := &fakeIO{n: make([]uint64, 2), allowed: map[int32][]int32{
		1: {0, 1}, 2: {0, 1}, 3: {0, 1},
	}}
	p := New(io, 2, Enums{})
	p.Handle(Event{Kind: EvEnqueue, Pid: 1, Cpu: 0, Weight: 100, EnqSeq: 1})
	p.Handle(Event{Kind: EvEnqueue, Pid: 2, Cpu: 0, Weight: 100, EnqSeq: 1})
	p.Handle(Event{Kind: EvEnqueue, Pid: 3, Cpu: 0, Weight: 100, EnqSeq: 1})
	t2, t3 := p.tasks[2], p.tasks[3]
	p.removeQueued(t2)
	p.removeQueued(t3)
	t2.Req.Deadline = 4_000_000
	t3.Req.Deadline = 2_000_000
	p.queueRetry(t2, 0)
	p.queueRetry(t3, 0)
	p.pull(1)
	if got := io.cmds[len(io.cmds)-1].Pid; got != 3 {
		t.Fatalf("idle pull chose pid %d, want earliest eligible deadline 3", got)
	}
}
