package main

import (
	"fmt"
	"runtime/debug"
	"syscall"
)

func ioAffinity() ([]int32, error) {
	cpus, err := (backend{}).Affinity(0)
	if err != nil {
		return nil, err
	}
	if len(cpus) == 0 {
		return nil, fmt.Errorf("scheduler has no allowed CPU")
	}
	return cpus, nil
}

func prepareRuntime() error {
	if err := syscall.Mlockall(syscall.MCL_CURRENT | syscall.MCL_FUTURE); err != nil {
		return fmt.Errorf("lock scheduler memory (check RLIMIT_MEMLOCK): %w", err)
	}
	debug.SetGCPercent(400)
	return nil
}
