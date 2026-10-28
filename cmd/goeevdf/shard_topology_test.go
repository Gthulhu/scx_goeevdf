package main

import (
	"reflect"
	"testing"

	"github.com/Gthulhu/scx_goeevdf/eevdf"
)

func TestLLCGroupsSeparateNodesAndNoncontiguousCPUs(t *testing.T) {
	got := llcGroups(eevdf.Topology{
		LLC:  []int{0, 1, 0, 0, 1, 0},
		Node: []int{0, 0, 0, 1, 0, 1},
	})
	want := [][]int32{{0, 2}, {1, 4}, {3, 5}}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("LLC groups = %v, want %v", got, want)
	}
}

func TestSelectShardGroupsAutoThreshold(t *testing.T) {
	topo := eevdf.Topology{LLC: make([]int, 65), Node: make([]int, 65)}
	for cpu := 32; cpu < 65; cpu++ {
		topo.LLC[cpu] = 1
	}
	if groups, err := selectShardGroups(65, topo, "auto"); err != nil ||
		len(groups) != 2 || len(groups[0]) != 32 || len(groups[1]) != 33 {
		t.Fatalf("auto groups = %v, %v", groups, err)
	}
	if groups, err := selectShardGroups(65, topo, "1"); err != nil || groups != nil {
		t.Fatalf("forced single owner = %v, %v", groups, err)
	}
	if groups, err := selectShardGroups(64, topo, "auto"); err != nil || groups != nil {
		t.Fatalf("64-CPU auto = %v, %v", groups, err)
	}
	if _, err := selectShardGroups(65, topo, "2"); err == nil {
		t.Fatal("unsupported shard mode accepted")
	}
}
