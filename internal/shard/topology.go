// Package shard routes scheduler events to per-LLC policy owners. The BPF
// per-shard rings and Policy task export/import hooks must be supplied by the
// caller; this package does not turn the existing single-owner Policy into a
// sharded policy.
package shard

import (
	"fmt"
)

// Topology maps every possible CPU to exactly one shard. CPUs are dense BPF
// indices, not the number of online CPUs. LLC groups must be in topology order.
type Topology struct {
	CPUs   [][]int32
	owners []int
}

// NewTopology builds the auto configuration: <=64 CPUs use one owner;
// otherwise adjacent LLCs smaller than four CPUs are coalesced. forceOne
// overrides auto mode. Every possible CPU must occur exactly once.
func NewTopology(possible int, llcs [][]int32, forceOne bool) (Topology, error) {
	if possible <= 0 || possible > 1024 {
		return Topology{}, fmt.Errorf("possible CPUs must be in [1,1024]")
	}
	seen := make([]bool, possible)
	for _, llc := range llcs {
		if len(llc) == 0 {
			return Topology{}, fmt.Errorf("empty LLC")
		}
		for _, cpu := range llc {
			if cpu < 0 || int(cpu) >= possible || seen[cpu] {
				return Topology{}, fmt.Errorf("invalid or duplicate CPU %d", cpu)
			}
			seen[cpu] = true
		}
	}
	for cpu, ok := range seen {
		if !ok {
			return Topology{}, fmt.Errorf("missing possible CPU %d", cpu)
		}
	}
	t := Topology{owners: make([]int, possible)}
	if forceOne || possible <= 64 {
		t.CPUs = make([][]int32, 1)
		for cpu := range seen {
			t.CPUs[0] = append(t.CPUs[0], int32(cpu))
		}
	} else {
		for _, llc := range llcs {
			if len(t.CPUs) == 0 || len(t.CPUs[len(t.CPUs)-1]) >= 4 {
				t.CPUs = append(t.CPUs, nil)
			}
			t.CPUs[len(t.CPUs)-1] = append(t.CPUs[len(t.CPUs)-1], llc...)
		}
		if len(t.CPUs) > 1 && len(t.CPUs[len(t.CPUs)-1]) < 4 {
			last := len(t.CPUs) - 1
			t.CPUs[last-1] = append(t.CPUs[last-1], t.CPUs[last]...)
			t.CPUs = t.CPUs[:last]
		}
	}
	if len(t.CPUs) > 32 {
		return Topology{}, fmt.Errorf("%d shards exceed BPF maximum 32", len(t.CPUs))
	}
	for id, cpus := range t.CPUs {
		for _, cpu := range cpus {
			t.owners[cpu] = id
		}
	}
	return t, nil
}

func (t Topology) OwnerCPU(cpu int32) (int, bool) {
	if cpu < 0 || int(cpu) >= len(t.owners) {
		return 0, false
	}
	return t.owners[cpu], true
}
