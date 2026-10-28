package main

import (
	"fmt"

	"github.com/Gthulhu/scx_goeevdf/eevdf"
	"github.com/Gthulhu/scx_goeevdf/internal/shard"
)

// llcGroups preserves CPU order while keeping identical LLC IDs on different
// NUMA nodes in separate groups.
func llcGroups(t eevdf.Topology) [][]int32 {
	type key struct{ node, llc int }
	ids := make(map[key]int)
	var groups [][]int32
	for cpu, llc := range t.LLC {
		k := key{t.Node[cpu], llc}
		id, ok := ids[k]
		if !ok {
			id = len(groups)
			ids[k] = id
			groups = append(groups, nil)
		}

		groups[id] = append(groups[id], int32(cpu))
	}
	return groups
}

// selectShardGroups returns nil when the single-owner scheduler should run.
func selectShardGroups(possible int, topology eevdf.Topology, mode string) ([][]int32, error) {
	switch mode {
	case "1":
		return nil, nil
	case "auto":
		if possible <= 64 {
			return nil, nil
		}
	default:
		return nil, fmt.Errorf("invalid --shards %q: use auto or 1", mode)
	}
	grouped, err := shard.NewTopology(possible, llcGroups(topology), false)
	if err != nil {
		return nil, err
	}
	if len(grouped.CPUs) == 1 {
		return nil, nil
	}
	return grouped.CPUs, nil
}
