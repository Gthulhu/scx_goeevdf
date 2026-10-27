package main

import (
	"os"
	"path/filepath"
	"testing"
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
