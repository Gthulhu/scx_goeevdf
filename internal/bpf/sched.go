// Package bpf exposes the sched_ext event/command transport. Build the BPF
// object with `make bpf` (clang, bpftool and libbpf-dev are required).
// Open or OpenPath, SetUserschedTgid(os.Getpid()), Load and Attach in that
// order, then Consume events and Submit dispatch commands; Kick a CPU to make
// submitted commands run promptly. Stop the event loop before Close. A kernel
// supporting sched_ext and privileges to load/attach BPF programs are required.
package bpf

/*
#cgo LDFLAGS: -lbpf -lelf -lz
#include "bridge.h"
#include <stdlib.h>
*/
import "C"

import (
	"errors"
	"fmt"
	"os"
	"runtime"
	"sync/atomic"
	"syscall"
	"unsafe"
)

// Sched owns the attached scheduler. Close only after the event loop exits.
// Consume and Submit are intended for one scheduler goroutine locked to an OS thread.
type Sched struct {
	handle *C.struct_ge_handle
	CPU    []CpuSlot // BPF writes; use atomic loads for concurrently changing fields.
	Ctl    []CpuCtl  // Go writes; use atomic stores.
	Task   []TaskPub // Go writes; publish multi-field updates with an odd/even Seq.
	maps   [][]byte
	bss    []byte
}

// Open loads the default object path; GOEEVDF_BPF_OBJECT can override it.
func Open() (*Sched, error) {
	path := os.Getenv("GOEEVDF_BPF_OBJECT")
	if path == "" {
		path = "bpf/goeevdf.bpf.o"
	}
	return OpenPath(path)
}

func OpenPath(path string) (*Sched, error) {
	cpath := C.CString(path)
	defer C.free(unsafe.Pointer(cpath))
	h, err := C.ge_open(cpath)
	if h == nil {
		return nil, fmt.Errorf("open BPF object %q: %w", path, err)
	}
	return &Sched{handle: h}, nil
}

// SetUserschedTgid must precede Load; it exempts the Go process from external dispatch.
func (s *Sched) SetUserschedTgid(tgid uint32) error {
	return check(C.ge_set_tgid(s.handle, C.uint32_t(tgid)))
}

func check(code C.int) error {
	if code < 0 {
		return syscall.Errno(-code)
	}
	return nil
}

func mmapArray[T any](fd, n int) ([]T, []byte, error) {
	size := int(unsafe.Sizeof(*new(T))) * n
	page := os.Getpagesize()
	size = (size + page - 1) &^ (page - 1)
	data, err := syscall.Mmap(fd, 0, size, syscall.PROT_READ|syscall.PROT_WRITE, syscall.MAP_SHARED)
	if err != nil {
		return nil, nil, err
	}
	return unsafe.Slice((*T)(unsafe.Pointer(&data[0])), n), data, nil
}

func (s *Sched) mapTable(name string) int {
	cname := C.CString(name)
	defer C.free(unsafe.Pointer(cname))
	return int(C.ge_fd(s.handle, cname))
}

func (s *Sched) Load() error {
	if err := check(C.ge_load(s.handle)); err != nil {
		return fmt.Errorf("load BPF: %w", err)
	}
	var data []byte
	var err error
	if s.CPU, data, err = mmapArray[CpuSlot](s.mapTable("cpu_tbl"), MaxCPUs); err != nil {
		return err
	}
	s.maps = append(s.maps, data)
	if s.Ctl, data, err = mmapArray[CpuCtl](s.mapTable("ctl_tbl"), MaxCPUs); err != nil {
		return err
	}
	s.maps = append(s.maps, data)
	if s.Task, data, err = mmapArray[TaskPub](s.mapTable("task_tbl"), MaxTasks); err != nil {
		return err
	}
	s.maps = append(s.maps, data)
	size := int(C.ge_bss_size(s.handle))
	if size <= 0 {
		return errors.New("BPF .bss map missing")
	}
	page := os.Getpagesize()
	s.bss, err = syscall.Mmap(s.mapTable(".bss"), 0, (size+page-1)&^(page-1), syscall.PROT_READ|syscall.PROT_WRITE, syscall.MAP_SHARED)
	return err
}

func (s *Sched) Attach() error {
	if err := check(C.ge_attach(s.handle)); err != nil {
		return fmt.Errorf("attach sched_ext: %w", err)
	}
	return nil
}

// Consume reads at most one event without blocking. The caller must not run
// another Consume on this Sched concurrently.
func (s *Sched) Consume(out *Ev) (bool, error) {
	code := C.ge_consume(s.handle, (*C.struct_ev)(unsafe.Pointer(out)))
	runtime.KeepAlive(s)
	if code < 0 {
		return false, check(code)
	}
	return code == 1, nil
}

// Submit queues a command and kicks its target CPU to trigger BPF dispatch.
// ENOSPC means the user ring is full. A kick failure does not invalidate a
// submitted command, so Submit must not report it as an error (the next
// dispatch callback will still drain the ring).
func (s *Sched) Submit(c *Cmd) error {
	code := C.ge_submit(s.handle, (*C.struct_cmd)(unsafe.Pointer(c)))
	runtime.KeepAlive(s)
	if err := check(code); err != nil {
		return err
	}
	flags := uint64(1) // SCX_KICK_IDLE
	if c.Kind == CmdStop || (c.Kind == CmdDispatch && c.EnqFlags != 0) {
		flags = 2 // SCX_KICK_PREEMPT
	}
	_ = s.Kick(c.Cpu, flags)
	return nil
}

// Kick executes a BPF syscall program to force a target CPU's dispatch callback.
func (s *Sched) Kick(cpu int32, flags uint64) error {
	return check(C.ge_kick(s.handle, C.int32_t(cpu), C.uint64_t(flags)))
}

func (s *Sched) testEmit(cpu int32, sequence uint64) error {
	return check(C.ge_test_emit(s.handle, C.int32_t(cpu), C.uint64_t(sequence)))
}

func (s *Sched) counter(name string) *uint64 {
	cname := C.CString(name)
	defer C.free(unsafe.Pointer(cname))
	off := int(C.ge_counter_offset(s.handle, cname))
	if off < 0 || off+8 > len(s.bss) || off%8 != 0 {
		return nil
	}
	return (*uint64)(unsafe.Pointer(&s.bss[off]))
}

func (s *Sched) Counter(name string) uint64 {
	if p := s.counter(name); p != nil {
		return atomic.LoadUint64(p)
	}
	return 0
}
func (s *Sched) DecNrQueued(n uint64) {
	if p := s.counter("nr_queued"); p != nil {
		for {
			old := atomic.LoadUint64(p)
			if atomic.CompareAndSwapUint64(p, old, old-min(old, n)) {
				return
			}
		}
	}
}
func (s *Sched) SetNrScheduled(n uint64) {
	if p := s.counter("nr_scheduled"); p != nil {
		atomic.StoreUint64(p, n)
	}
}

func (s *Sched) Yield() error {
	_, _, errno := syscall.RawSyscall(syscall.SYS_SCHED_YIELD, 0, 0, 0)
	if errno != 0 {
		return errno
	}
	return nil
}

// Close detaches before unmapping tables; it is not safe concurrently with use.
func (s *Sched) Close() {
	if s == nil || s.handle == nil {
		return
	}
	C.ge_close(s.handle)
	s.handle = nil
	if s.bss != nil {
		_ = syscall.Munmap(s.bss)
	}
	for _, data := range s.maps {
		_ = syscall.Munmap(data)
	}
	s.CPU, s.Ctl, s.Task, s.maps, s.bss = nil, nil, nil, nil, nil
}
