package main

import (
	"os"
	"path/filepath"
	"testing"
)

func TestReadTopology(t *testing.T) {
	root := t.TempDir()
	for c, values := range []struct {
		pkg, core, llc, node, capacity string
	}{
		{"0", "0", "1", "0", "1024"},
		{"0", "0", "1", "0", "1024"},
		{"0", "1", "2", "0", "512"},
		{"1", "0", "3", "1", "1024"},
	} {
		cpu := filepath.Join(root, "cpu"+string(rune('0'+c)))
		for name, value := range map[string]string{
			"topology/physical_package_id": values.pkg,
			"topology/core_id":             values.core,
			"cache/index3/id":              values.llc,
			"cache/index3/level":           "3",
			"cpu_capacity":                 values.capacity,
		} {
			path := filepath.Join(cpu, name)
			if err := os.MkdirAll(filepath.Dir(path), 0700); err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(path, []byte(value), 0600); err != nil {
				t.Fatal(err)
			}
		}
		if err := os.MkdirAll(filepath.Join(cpu, "node"+values.node), 0700); err != nil {
			t.Fatal(err)
		}
	}
	topo := readTopology(root, 4)
	if topo.Core[0] != topo.Core[1] || topo.Core[0] == topo.Core[3] ||
		topo.LLC[0] != topo.LLC[1] || topo.LLC[1] == topo.LLC[2] ||
		topo.Node[2] != topo.Node[0] || topo.Node[3] == topo.Node[0] ||
		topo.Capacity[2] != 512 {
		t.Fatalf("incorrect sysfs topology: %+v", topo)
	}
}
