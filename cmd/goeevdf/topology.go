package main

import (
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/Gthulhu/scx_goeevdf/eevdf"
)

func readTopology(root string, cpus int) eevdf.Topology {
	t := eevdf.Topology{
		Core: make([]int, cpus), LLC: make([]int, cpus),
		Node: make([]int, cpus), Capacity: make([]uint64, cpus),
	}
	for c := 0; c < cpus; c++ {
		cpuDir := filepath.Join(root, fmt.Sprintf("cpu%d", c))
		packageID := readTopologyID(filepath.Join(cpuDir, "topology/physical_package_id"), c)
		coreID := readTopologyID(filepath.Join(cpuDir, "topology/core_id"), c)
		// Use a pair to distinguish identical core IDs on different packages.
		t.Core[c] = packageID*cpus + coreID
		t.Node[c] = packageID
		if matches, _ := filepath.Glob(filepath.Join(cpuDir, "node[0-9]*")); len(matches) != 0 {
			if id, err := strconv.Atoi(strings.TrimPrefix(filepath.Base(matches[0]), "node")); err == nil {
				t.Node[c] = id
			}
		}
		t.LLC[c] = packageID
		if entries, err := filepath.Glob(filepath.Join(cpuDir, "cache/index*")); err == nil {
			level, cacheID := -1, -1
			for _, entry := range entries {
				l := readTopologyID(filepath.Join(entry, "level"), -1)
				id := readTopologyID(filepath.Join(entry, "id"), -1)
				if l > level && id >= 0 {
					level, cacheID = l, id
				}
			}
			if cacheID >= 0 {
				t.LLC[c] = packageID*cpus + cacheID
			}
		}
		t.Capacity[c] = uint64(readTopologyID(filepath.Join(cpuDir, "cpu_capacity"), 1024))
		if t.Capacity[c] == 0 {
			t.Capacity[c] = 1024
		}
	}
	return t
}

func readTopologyID(path string, fallback int) int {
	data, err := os.ReadFile(path)
	if err != nil {
		return fallback
	}
	n, err := strconv.Atoi(strings.TrimSpace(string(data)))
	if err != nil {
		return fallback
	}
	return n
}
