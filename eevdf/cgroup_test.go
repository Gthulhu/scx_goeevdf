package eevdf

import "testing"

func TestNewUsesCPUScaledBaseSlice(t *testing.T) {
	for _, tc := range []struct {
		cpus int
		want uint64
	}{{1, 700_000}, {4, 2_100_000}, {8, 2_800_000}} {
		io := &fakeIO{n: make([]uint64, tc.cpus)}
		if got := New(io, tc.cpus, Enums{}).Slice; got != tc.want {
			t.Errorf("%d CPUs: slice = %d, want %d", tc.cpus, got, tc.want)
		}
	}
}

func TestCgroupEffectiveWeights(t *testing.T) {
	io := &fakeIO{n: make([]uint64, 1)}
	p := New(io, 1, Enums{})
	for _, cfg := range [][3]uint64{{1, 0, 100}, {2, 1, 100}, {3, 0, 200}} {
		if err := p.SetCgroup(cfg[0], cfg[1], cfg[2]); err != nil {
			t.Fatal(err)
		}
	}
	for _, task := range [][2]uint64{{1, 2}, {2, 2}, {3, 1}, {4, 3}, {5, 0}} {
		pid := int32(task[0])
		p.Handle(Event{Kind: EvEnqueue, Pid: pid, Cpu: 0, Cgid: task[1],
			Weight: 100, EnqSeq: 1})
	}
	// Group 1 and group 3 each contribute one entity at root. Group 1
	// divides its shares between its direct task and its nested queue.
	for pid, want := range map[int32]uint64{1: 256, 2: 256, 3: 512, 4: 2048, 5: 1024} {
		if got := p.tasks[pid].Weight; got != want {
			t.Errorf("pid %d effective weight = %d, want %d", pid, got, want)
		}
	}
	p.Handle(Event{Kind: EvSleep, Pid: 2})
	if got := p.tasks[1].Weight; got != 512 {
		t.Fatalf("remaining group member weight = %d", got)
	}
	p.Handle(Event{Kind: EvEnqueue, Pid: 1, Cpu: 0, Cgid: 3, Weight: 100, EnqSeq: 2})
	if got := p.tasks[1].Weight; got != 1024 {
		t.Fatalf("moved task effective weight = %d", got)
	}
	if got := p.tasks[4].Weight; got != 1024 {
		t.Fatalf("new group sibling effective weight = %d", got)
	}
}

func TestCgroupWeightChangeAndValidation(t *testing.T) {
	io := &fakeIO{n: make([]uint64, 1)}
	p := New(io, 1, Enums{})
	if err := p.SetCgroup(2, 1, 100); err == nil {
		t.Fatal("unknown parent accepted")
	}

	if err := p.SetCgroup(1, 0, 100); err != nil {
		t.Fatal(err)
	}

	if err := p.SetCgroup(2, 1, 100); err != nil {
		t.Fatal(err)
	}
	if err := p.SetCgroup(1, 2, 100); err == nil {
		t.Fatal("cycle accepted")
	}
	p.Handle(Event{Kind: EvEnqueue, Pid: 1, Cpu: 0, Weight: 100, Cgid: 1})
	p.Handle(Event{Kind: EvEnqueue, Pid: 2, Cpu: 0, Weight: 100, Cgid: 2})
	if p.tasks[1].Weight != 512 || p.tasks[2].Weight != 512 {
		t.Fatal("nested queue must compete with direct parent task")
	}
	if err := p.SetCgroup(2, 1, 200); err != nil {
		t.Fatal(err)
	}
	if p.tasks[1].Weight != 341 || p.tasks[2].Weight != 683 {
		t.Fatalf("updated weights %d and %d", p.tasks[1].Weight, p.tasks[2].Weight)
	}
}

func TestRemoveCgroupRestoresRootWeight(t *testing.T) {
	io := &fakeIO{n: make([]uint64, 1)}
	p := New(io, 1, Enums{})
	if err := p.SetCgroup(1, 0, 100); err != nil {
		t.Fatal(err)
	}

	if err := p.SetCgroup(2, 1, 100); err != nil {
		t.Fatal(err)
	}
	p.Handle(Event{Kind: EvEnqueue, Pid: 1, Cpu: 0, Weight: 100, Cgid: 1})
	p.Handle(Event{Kind: EvEnqueue, Pid: 2, Cpu: 0, Weight: 100, Cgid: 2})
	p.RemoveCgroup(1)
	if p.groups[2].Parent != nil || p.tasks[1].Weight != Nice0Weight ||
		p.tasks[2].Weight != Nice0Weight {
		t.Fatal("removing cgroup did not reparent descendants and tasks")
	}
}

func TestCgroupCPUShare(t *testing.T) {
	io := &fakeIO{n: make([]uint64, 1)}
	p := New(io, 1, Enums{})
	for id := uint64(1); id <= 2; id++ {
		if err := p.SetCgroup(id, 0, 100); err != nil {
			t.Fatal(err)
		}
	}
	for pid, id := range []uint64{1, 1, 2} {
		p.Handle(Event{Kind: EvEnqueue, Pid: int32(pid + 1), Cpu: 0,
			Weight: 100, Cgid: id, EnqSeq: 1})
	}
	var runtime [4]uint64
	var sequence [4]uint64
	sequence[1], sequence[2], sequence[3] = 1, 1, 1
	for i := 0; i < 900; i++ {
		pid := io.cmds[len(io.cmds)-1].Pid
		runtime[pid] += p.Slice
		sequence[pid]++
		cgid := uint64(1)
		if pid == 3 {
			cgid = 2
		}
		p.Handle(Event{Kind: EvEnqueue, Pid: pid, Cpu: 0, Weight: 100,
			Cgid: cgid, SumExec: runtime[pid], EnqSeq: sequence[pid]})
		p.Schedule()
	}
	ratio := float64(runtime[3]) / float64(runtime[1]+runtime[2])
	if ratio < 0.9 || ratio > 1.1 {
		t.Fatalf("group share ratio %.2f (want ~1): %v", ratio, runtime)
	}
}

func TestCpumaskMigratesQueuedTask(t *testing.T) {
	io := &fakeIO{n: make([]uint64, 2), allowed: map[int32][]int32{
		1: {0}, 2: {0},
	}}
	p := New(io, 2, Enums{})
	p.Handle(Event{Kind: EvEnqueue, Pid: 1, Cpu: 0, Weight: 100})
	p.Handle(Event{Kind: EvEnqueue, Pid: 2, Cpu: 0, Weight: 100})
	if p.tasks[2].state != Queued || p.packs[0].W != 2*Nice0Weight {
		t.Fatal("expected second task queued on original CPU")
	}
	io.allowed[2] = []int32{1}
	p.Handle(Event{Kind: EvCpumask, Pid: 2})
	if p.tasks[2].Cpu != 1 || p.tasks[2].pack != p.packs[1] ||
		p.packs[0].W != Nice0Weight || p.packs[1].W != Nice0Weight ||
		p.trees[0].Len() != 0 || io.n[0] != 0 ||
		len(io.cmds) != 2 || io.cmds[1].Pid != 2 || io.cmds[1].Cpu != 1 {
		t.Fatalf("queued task was not moved to allowed CPU: %+v", io.cmds)
	}
}

func TestCpumaskWithoutUsableCPURetainsQueuedTask(t *testing.T) {
	io := &fakeIO{n: make([]uint64, 2), allowed: map[int32][]int32{
		1: {0}, 2: {0},
	}}
	p := New(io, 2, Enums{})
	p.Handle(Event{Kind: EvEnqueue, Pid: 1, Cpu: 0, Weight: 100})
	p.Handle(Event{Kind: EvEnqueue, Pid: 2, Cpu: 0, Weight: 100})
	io.allowed[2] = []int32{5}
	p.Handle(Event{Kind: EvCpumask, Pid: 2})
	if p.tasks[2].Cpu != 0 || p.tasks[2].state != Queued ||
		p.trees[0].Len() != 1 || p.packs[0].W != 2*Nice0Weight {
		t.Fatal("task with no usable CPU was lost")
	}
}

func TestUnknownCgroupDefaultsToRootUntilConfigured(t *testing.T) {
	io := &fakeIO{n: make([]uint64, 1)}
	p := New(io, 1, Enums{})
	p.Handle(Event{Kind: EvEnqueue, Pid: 1, Cpu: 0, Cgid: 42, Weight: 100})
	if task := p.tasks[1]; task.Cgid != 42 || task.Weight != Nice0Weight ||
		p.group(task) != nil {
		t.Fatal("unknown cgroup did not retain ID with root scheduling weight")
	}
	if err := p.SetCgroup(42, 0, 200); err != nil {
		t.Fatal(err)
	}
	if p.tasks[1].Weight != cgroupLoadWeight(200) {
		t.Fatal("late cgroup provisioning did not reweight runnable task")
	}
}

func TestReweightPreservesWeightedLag(t *testing.T) {
	for _, tc := range []struct {
		name      string
		vtime     uint64
		newWeight uint64
		wantLag   int64
	}{
		{"credit on weight increase", 900, 2048, 50},
		{"debt on weight decrease", 1100, 512, -200},
	} {
		t.Run(tc.name, func(t *testing.T) {
			p := New(&fakeIO{n: make([]uint64, 1)}, 1, Enums{})
			pk := p.packs[0]
			a := &Task{Pid: 1, Cpu: 0, Vruntime: tc.vtime,
				Nice: Nice0Weight, Weight: Nice0Weight, initial: true}
			b := &Task{Pid: 2, Cpu: 0, Vruntime: 2000 - tc.vtime,
				Nice: Nice0Weight, Weight: Nice0Weight, initial: true}
			pk.join(a)
			pk.join(b)
			if got := pk.lag(a, p.Slice); got != tc.wantLag*int64(tc.newWeight)/int64(a.Weight) {
				t.Fatalf("unexpected initial lag %d", got)
			}
			p.reweight(a, tc.newWeight)
			if got := pk.lag(a, p.Slice); got != tc.wantLag {
				t.Fatalf("lag after reweight = %d, want %d", got, tc.wantLag)
			}
			if pk.W != tc.newWeight+Nice0Weight {
				t.Fatalf("pack weight after reweight = %d", pk.W)
			}
		})
	}
}
