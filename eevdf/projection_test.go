package eevdf

import "testing"

type projectionIO struct{ *fakeIO }

func (i projectionIO) CurrPid(int32) int32   { return 1 }
func (i projectionIO) RunStart(int32) uint64 { return 100 }

func TestReferenceProjectionFeature(t *testing.T) {
	io := projectionIO{&fakeIO{n: make([]uint64, 1)}}
	p := New(io, 1, Enums{})
	tk := &Task{Pid: 1, Cpu: 0, Weight: Nice0Weight, Vruntime: 100,
		Req: Request{Slice: 1000}}
	p.packs[0].join(tk)
	p.cpus[0].task, p.cpus[0].since = tk, 100
	if got := p.refAt(p.packs[0], 200); got != 200 {
		t.Fatalf("lone projection = %d, want 200", got)
	}
	if tk.Vruntime != 100 || p.packs[0].Ref != 100 {
		t.Fatal("projection charged service instead of reading it")
	}
	p.F.VrefUpdate = false
	if got := p.refAt(p.packs[0], 200); got != 100 {
		t.Fatalf("disabled projection = %d", got)
	}
	p.F.VrefUpdate = true
	other := &Task{Pid: 2, Weight: Nice0Weight, Vruntime: 100}
	p.packs[0].join(other)
	if got := p.refAt(p.packs[0], 200); got != 150 {
		t.Fatalf("weighted projection = %d", got)
	}
	p.cpus[0].since = 150
	if got := p.refAt(p.packs[0], 200); got != 125 {
		t.Fatalf("already accounted interval projected twice: %d", got)
	}
}

func TestPlacementUsesProjectedReference(t *testing.T) {
	for _, enabled := range []bool{false, true} {
		p := New(projectionIO{&fakeIO{n: make([]uint64, 1)}}, 1, Enums{})
		p.F.VrefUpdate = enabled
		p.now = 200
		cur := &Task{Pid: 1, Weight: Nice0Weight, Vruntime: 100,
			Req: Request{Slice: 1000}}
		p.packs[0].join(cur)
		p.cpus[0].task, p.cpus[0].since = cur, 100
		wakee := &Task{Pid: 2, Weight: Nice0Weight}
		p.placeTask(wakee, 0, true)
		want := uint64(100)
		if enabled {
			want = 200
		}
		if wakee.Vruntime != want || cur.Vruntime != 100 {
			t.Fatalf("projection=%v: wakee=%d current=%d", enabled, wakee.Vruntime, cur.Vruntime)
		}
	}
}
