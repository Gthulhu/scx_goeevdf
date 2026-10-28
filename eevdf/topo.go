package eevdf

import "fmt"

const (
	lvLLC = iota
	lvNode
	lvSystem
	taskScan = 8
)

// Topology describes dense policy CPU IDs. Equal IDs identify shared cores,
// last-level caches and NUMA nodes; capacities are in scheduler units.
type Topology struct {
	Core, LLC, Node   []int
	Capacity          []uint64
	SMT, AsymCapacity bool
}

func defaultTopology(n int) Topology {
	t := Topology{
		Core: make([]int, n), LLC: make([]int, n),
		Node: make([]int, n), Capacity: make([]uint64, n),
	}
	for c := range t.Core {
		t.Core[c] = c
		t.Capacity[c] = Nice0Weight
	}
	return t
}

// SetTopology replaces the placement and balancing topology. Call from the
// policy's owning goroutine before scheduling tasks.
func (p *Policy) SetTopology(t Topology) error {
	n := len(p.cpus)
	if len(t.Core) != n || len(t.LLC) != n || len(t.Node) != n || len(t.Capacity) != n {
		return fmt.Errorf("topology must describe all %d CPUs", n)
	}
	for c := 0; c < n; c++ {
		if t.Capacity[c] == 0 {
			return fmt.Errorf("CPU %d has zero capacity", c)
		}
		for x := 0; x < c; x++ {
			if t.Core[c] == t.Core[x] && (t.LLC[c] != t.LLC[x] || t.Node[c] != t.Node[x]) ||
				t.LLC[c] == t.LLC[x] && t.Node[c] != t.Node[x] {
				return fmt.Errorf("inconsistent topology for CPUs %d and %d", x, c)
			}
		}
	}
	t.Core = append([]int(nil), t.Core...)
	t.LLC = append([]int(nil), t.LLC...)
	t.Node = append([]int(nil), t.Node...)
	t.Capacity = append([]uint64(nil), t.Capacity...)
	t.SMT, t.AsymCapacity = false, false
	for c := 0; c < n; c++ {
		if t.Capacity[c] != t.Capacity[0] {
			t.AsymCapacity = true
		}
		for x := 0; x < c; x++ {
			if t.Core[x] == t.Core[c] {
				t.SMT = true
			}
		}
	}
	p.topo = t
	return nil
}

func (t Topology) sameCore(a, b int32) bool { return t.Core[a] == t.Core[b] }
func (t Topology) sameLLC(a, b int32) bool  { return t.LLC[a] == t.LLC[b] }
func (t Topology) sameNode(a, b int32) bool { return t.Node[a] == t.Node[b] }

// newidleSpans partitions donor CPUs by increasing distance from dst.
func (t Topology) newidleSpans(dst int32) [3][]int32 {
	var spans [3][]int32
	for c := range t.Core {
		cpu := int32(c)
		if cpu == dst {
			continue
		}
		lv := lvSystem
		if t.sameLLC(cpu, dst) {
			lv = lvLLC
		} else if t.sameNode(cpu, dst) {
			lv = lvNode
		}
		spans[lv] = append(spans[lv], cpu)
	}
	return spans
}
