package main

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/Gthulhu/scx_goeevdf/eevdf"
)

func TestReadQuota(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "cpu.max")
	for _, tc := range []struct {
		text          string
		quota, period uint64
		wantErr       bool
	}{
		{"max 100000\n", 0, 100_000_000, false},
		{"20000 100000\n", 20_000_000, 100_000_000, false},
		{"20 0\n", 0, 0, true},
		{"-1 100000\n", 0, 0, true},
		{"invalid\n", 0, 0, true},
	} {
		if err := os.WriteFile(path, []byte(tc.text), 0600); err != nil {
			t.Fatal(err)
		}
		quota, period, err := readQuota(dir)
		if (err != nil) != tc.wantErr || (!tc.wantErr &&
			(quota != tc.quota || period != tc.period)) {
			t.Fatalf("readQuota(%q) = %d, %d, %v", tc.text, quota, period, err)
		}
	}
}

func TestReadIdle(t *testing.T) {
	dir := t.TempDir()
	if idle, err := readIdle(dir); err != nil || idle {
		t.Fatalf("missing cpu.idle: idle=%v error=%v", idle, err)
	}
	for _, tc := range []struct {
		text    string
		want    bool
		wantErr bool
	}{{"0\n", false, false}, {"1\n", true, false}, {" 1 \n", true, false},
		{"2\n", false, true}, {"invalid\n", false, true}, {"", false, true}} {
		if err := os.WriteFile(filepath.Join(dir, "cpu.idle"), []byte(tc.text), 0600); err != nil {
			t.Fatal(err)
		}
		if got, err := readIdle(dir); got != tc.want || (err != nil) != tc.wantErr {
			t.Errorf("readIdle(%q) = %v, %v; want %v, error=%v", tc.text, got, err, tc.want, tc.wantErr)
		}
	}
}

func TestConfigureRootCgroup(t *testing.T) {
	const root = "/sys/fs/cgroup"
	id, err := cgroupID(root)
	if err != nil {
		t.Skipf("cgroup v2 root unavailable: %v", err)
	}
	p := eevdf.New(&frequencyIO{perf: make([]uint64, 1)}, 1, eevdf.Enums{})
	var groups groupSync
	if err := groups.configure(p, root); err != nil {
		t.Fatal(err)
	}
	config, ok := groups.applied[id]
	if !ok || config.parent != 0 {
		t.Fatalf("root cgroup not configured: %+v", groups.applied)
	}
	if config.idle {
		t.Fatal("root cgroup configured as idle")
	}
	if err := p.SetCgroupIdle(id, true); err == nil {
		t.Fatal("cgroupfs root was registered as a weighted group")
	}
	if quota, period, err := readQuota(root); err == nil &&
		(config.quota != quota || config.period != period) {
		t.Fatalf("root quota = %d/%d, want %d/%d",
			config.quota, config.period, quota, period)
	}
}
