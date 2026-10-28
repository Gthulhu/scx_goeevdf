package main

import (
	"fmt"
	"os"

	"github.com/Gthulhu/scx_goeevdf/eevdf"
	"github.com/Gthulhu/scx_goeevdf/internal/bpf"
)

func policyEnums(e bpf.KernelEnums) eevdf.Enums {
	return eevdf.Enums{
		EnqWakeup: e.EnqWakeup, EnqPreempt: e.EnqPreempt, EnqBlocked: e.EnqBlocked,
		WakeSync: e.WakeSync, WakeFork: e.WakeFork,
		WakeCurrentCPU: e.WakeCurrentCPU, WakeTTWU: e.WakeTTWU,
	}
}

func runtimeEnums(features eevdf.Features) (bpf.KernelEnums, eevdf.Features, error) {
	enums, err := bpf.ImportEnums()
	if err != nil {
		return enums, features, err
	}
	if features.ProxyExec && enums.EnqBlocked == 0 {
		fmt.Fprintln(os.Stderr, "goeevdf: kernel has no SCX_ENQ_BLOCKED; disabling proxy execution")
		features.ProxyExec = false
	}
	return enums, features, nil
}
