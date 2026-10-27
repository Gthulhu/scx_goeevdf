package eevdf

import (
	"math"
	"math/rand/v2"
	"sort"
	"testing"
)

func TestTreeWrapEligibilityAndDuplicates(t *testing.T) {
	var a, b Tree
	tasks := []*Task{
		{Vruntime: math.MaxUint64 - 2, Req: Request{Deadline: math.MaxUint64 - 1, Slice: 10}, seq: 1},
		{Vruntime: 0, Req: Request{Deadline: 1, Slice: 5}, seq: 2},
		{Vruntime: 2, Req: Request{Deadline: 3, Slice: 20}, seq: 3},
	}
	for _, task := range tasks {
		a.Insert(task)
		a.Insert(task)
		b.Insert(task)
	}
	if a.Len() != 3 || b.Len() != 0 || a.Min() != tasks[0] || a.MinSlice() != 5 {
		t.Fatal("duplicate insert, ordering, or subtree slice")
	}
	if a.PickEligible(math.MaxUint64-3) != nil || a.PickEligible(0) != tasks[0] {
		t.Fatal("eligibility around wrap")
	}
	b.Remove(tasks[0])
	if a.Len() != 3 || tasks[0].node == nil {
		t.Fatal("wrong-tree remove corrupted ownership")
	}
	a.Remove(tasks[0])
	if a.Len() != 2 || a.Min() != tasks[1] {
		t.Fatal("remove did not update ordering")
	}
	b.Insert(tasks[0])
	if b.Len() != 1 || b.Min() != tasks[0] {
		t.Fatal("removed task could not join another tree")
	}
	if got := a.firstN(1, func(t *Task) bool { return t == tasks[2] }); got != nil {
		t.Fatalf("firstN searched past bound: %p", got)
	}
	if got := a.firstN(2, func(t *Task) bool { return t == tasks[2] }); got != tasks[2] {
		t.Fatal("firstN did not scan in deadline order")
	}
}

func TestTreeEqualKeys(t *testing.T) {
	var tr Tree
	tasks := make([]*Task, 30)
	for i := range tasks {
		tasks[i] = &Task{Req: Request{Deadline: 42, Slice: uint64(i + 1)}}
		tr.Insert(tasks[i])
	}
	for i, task := range tasks {
		if tr.Min() != task {
			t.Fatalf("duplicate key order at %d", i)
		}
		tr.Remove(task)
		if tr.Len() != len(tasks)-i-1 {
			t.Fatal("duplicate key removal lost another task")
		}
	}
}

func TestTreeAgainstSortedScan(t *testing.T) {
	rng := rand.New(rand.NewPCG(1234, 5678))
	const base = uint64(math.MaxUint64 - 500)
	var tr Tree
	active := make(map[*Task]bool)
	for step := 0; step < 100000; step++ {
		if len(active) == 0 || len(active) < 70 && rng.IntN(2) == 0 {
			task := &Task{
				Vruntime: base + uint64(rng.IntN(1000)),
				Req:      Request{Deadline: base + uint64(rng.IntN(1000)), Slice: uint64(rng.IntN(100) + 1)},
				seq:      uint64(step),
			}
			tr.Insert(task)
			active[task] = true
		} else {
			target := rng.IntN(len(active))
			for task := range active {
				if target == 0 {
					tr.Remove(task)
					delete(active, task)
					break
				}
				target--
			}
		}
		ordered := make([]*Task, 0, len(active))
		minSlice := uint64(math.MaxUint64)
		for task := range active {
			ordered = append(ordered, task)
			minSlice = min(minSlice, task.Req.Slice)
		}
		if len(ordered) == 0 {
			minSlice = 0
		}
		sort.Slice(ordered, func(i, j int) bool { return less(ordered[i], ordered[j]) })
		if tr.Len() != len(ordered) || tr.MinSlice() != minSlice {
			t.Fatalf("step %d: size/slice mismatch", step)
		}
		if len(ordered) == 0 {
			if tr.Min() != nil {
				t.Fatal("nonempty minimum for empty tree")
			}
			continue
		}
		if tr.Min() != ordered[0] {
			t.Fatalf("step %d: wrong minimum", step)
		}
		v := base + uint64(rng.IntN(1000))
		var want *Task
		for _, task := range ordered {
			if !TimeBefore(v, task.Vruntime) {
				want = task
				break
			}
		}
		if got := tr.PickEligible(v); got != want {
			t.Fatalf("step %d: eligible got %p want %p", step, got, want)
		}
		limit := rng.IntN(len(ordered) + 1)
		var first *Task
		for _, task := range ordered[:limit] {
			if task.Req.Slice%7 == 0 {
				first = task
				break
			}
		}
		if got := tr.firstN(limit, func(t *Task) bool { return t.Req.Slice%7 == 0 }); got != first {
			t.Fatalf("step %d: firstN got %p want %p", step, got, first)
		}
	}
}
