package main

import (
	"os"
	"testing"

	"github.com/Gthulhu/scx_goeevdf/internal/bpf"
)

func TestPossibleCPUsAndAffinity(t *testing.T) {
	ncpu, err := possibleCPUs()
	if err != nil {
		t.Fatal(err)
	}
	if ncpu <= 0 || ncpu > bpf.MaxCPUs {
		t.Fatalf("possible CPUs = %d", ncpu)
	}
	allowed, err := (backend{}).Affinity(int32(os.Getpid()))
	if err != nil || len(allowed) == 0 {
		t.Fatalf("sched_getaffinity: %v, CPUs %v", err, allowed)
	}
	for _, cpu := range allowed {
		if cpu < 0 || int(cpu) >= ncpu {
			t.Fatalf("allowed CPU %d outside possible range %d", cpu, ncpu)
		}
	}
}
