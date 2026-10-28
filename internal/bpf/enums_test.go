package bpf

import (
	"errors"
	"strings"
	"syscall"
	"testing"
)

func TestImportEnums(t *testing.T) {
	lookup := func(name string) (uint64, error) {
		if name == "SCX_ENQ_BLOCKED" || name == "SCX_WAKE_CURRENT_CPU" {
			return 0, syscall.ENOENT
		}
		return 1 << 12, nil
	}
	e, err := importEnums(lookup)
	if err != nil || e.EnqPreempt != 1<<12 || e.WakeFork != 1<<12 ||
		e.EnqBlocked != 0 || e.WakeCurrentCPU != 0 {
		t.Fatalf("imported flags: %+v, %v", e, err)
	}
	_, err = importEnums(func(string) (uint64, error) { return 0, syscall.ENOENT })
	if !errors.Is(err, syscall.ENOENT) || !strings.Contains(err.Error(), "SCX_ENQ_WAKEUP") {
		t.Fatalf("missing required flag: %v", err)
	}
	_, err = importEnums(func(name string) (uint64, error) {
		if name == "SCX_ENQ_BLOCKED" {
			return 0, syscall.EACCES
		}
		return 1, nil
	})
	if !errors.Is(err, syscall.EACCES) {
		t.Fatalf("optional lookup failure was hidden: %v", err)
	}
	_, err = importEnums(func(string) (uint64, error) { return 0, nil })
	if err == nil {
		t.Fatal("accepted a zero flag")
	}
}
