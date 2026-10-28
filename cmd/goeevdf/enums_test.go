package main

import (
	"testing"

	"github.com/Gthulhu/scx_goeevdf/internal/bpf"
)

func TestPolicyEnumsPreserveKernelValues(t *testing.T) {
	kernel := bpf.KernelEnums{
		EnqWakeup: 1 << 3, EnqPreempt: 1 << 40, EnqBlocked: 1 << 42,
		WakeSync: 1 << 5, WakeFork: 1 << 6, WakeCurrentCPU: 1 << 7, WakeTTWU: 1 << 8,
	}
	got := policyEnums(kernel)
	if got.EnqWakeup != kernel.EnqWakeup || got.EnqPreempt != kernel.EnqPreempt ||
		got.EnqBlocked != kernel.EnqBlocked || got.WakeSync != kernel.WakeSync ||
		got.WakeFork != kernel.WakeFork || got.WakeCurrentCPU != kernel.WakeCurrentCPU ||
		got.WakeTTWU != kernel.WakeTTWU {
		t.Fatalf("kernel values changed in policy configuration: %+v", got)
	}
}
