package eevdf

import "testing"

func TestTopologySpansAndValidation(t *testing.T) {
	p := New(&fakeIO{n: make([]uint64, 6)}, 6, Enums{})
	topo := Topology{
		Core:     []int{0, 0, 1, 2, 3, 4},
		LLC:      []int{0, 0, 0, 1, 1, 2},
		Node:     []int{0, 0, 0, 0, 0, 1},
		Capacity: []uint64{1024, 1024, 1024, 512, 512, 1024},
	}
	if err := p.SetTopology(topo); err != nil {
		t.Fatal(err)
	}
	spans := p.topo.newidleSpans(0)
	if len(spans[lvLLC]) != 2 || len(spans[lvNode]) != 2 ||
		len(spans[lvSystem]) != 1 || !p.topo.SMT || !p.topo.AsymCapacity {
		t.Fatalf("bad topology spans: %+v", spans)
	}
	topo.Node[1] = 2
	if err := p.SetTopology(topo); err == nil {
		t.Fatal("split core was accepted")
	}
}
