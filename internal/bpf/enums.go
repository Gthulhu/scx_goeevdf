package bpf

/*
#include "bridge.h"
#include <stdlib.h>
*/
import "C"

import (
	"errors"
	"fmt"
	"syscall"
	"unsafe"
)

// KernelEnums contains flags imported from the running kernel's BTF.
// Optional flags stay zero when unsupported; other lookup errors are fatal.
type KernelEnums struct {
	EnqWakeup, EnqPreempt, EnqBlocked            uint64
	WakeSync, WakeFork, WakeCurrentCPU, WakeTTWU uint64
	KickIdle, KickPreempt                        uint64
}

func readKernelEnum(name string) (uint64, error) {
	cname := C.CString(name)
	defer C.free(unsafe.Pointer(cname))
	var value C.uint64_t
	if err := check(C.ge_enum_value(cname, &value)); err != nil {
		return 0, err
	}
	return uint64(value), nil
}

func importEnums(lookup func(string) (uint64, error)) (KernelEnums, error) {
	var e KernelEnums
	for _, field := range []struct {
		name     string
		dst      *uint64
		optional bool
	}{
		{"SCX_ENQ_WAKEUP", &e.EnqWakeup, false},
		{"SCX_ENQ_PREEMPT", &e.EnqPreempt, false},
		{"SCX_ENQ_BLOCKED", &e.EnqBlocked, true},
		{"SCX_WAKE_SYNC", &e.WakeSync, false},
		{"SCX_WAKE_FORK", &e.WakeFork, false},
		{"SCX_WAKE_TTWU", &e.WakeTTWU, false},
		{"SCX_WAKE_CURRENT_CPU", &e.WakeCurrentCPU, true},
		{"SCX_KICK_IDLE", &e.KickIdle, false},
		{"SCX_KICK_PREEMPT", &e.KickPreempt, false},
	} {
		v, err := lookup(field.name)
		if field.optional && errors.Is(err, syscall.ENOENT) {
			continue
		}
		if err != nil {
			return e, fmt.Errorf("import %s from kernel BTF: %w", field.name, err)
		}
		if v == 0 {
			return e, fmt.Errorf("kernel BTF flag %s is zero", field.name)
		}
		*field.dst = v
	}
	return e, nil
}

func ImportEnums() (KernelEnums, error) { return importEnums(readKernelEnum) }
