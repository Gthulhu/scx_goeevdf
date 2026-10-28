// Package bpf exposes the sched_ext event/command transport. Build the BPF
// object with `make bpf` (clang, bpftool and libbpf-dev are required).
// Open or OpenPath, SetUserschedTgid(os.Getpid()), Load and Attach in that
// order, then Consume events and Submit dispatch commands; Kick a CPU to make
// submitted commands run promptly. Stop the event loop before Close. A kernel
// supporting sched_ext and privileges to load/attach BPF programs are required.
package bpf

/*
#cgo LDFLAGS: -l:libbpf.so.1 -lelf -lz
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

// Sched owns the attached scheduler. Close only after all shard loops exit.
// Each shard must have exactly one ConsumeShard consumer and SubmitShard producer.
type Sched struct {
	handle   *C.struct_ge_handle
	CPU      []CpuSlot // BPF writes; use atomic loads for concurrently changing fields.
	Ctl      []CpuCtl  // Go writes; use atomic stores.
	Task     []TaskPub // Go writes; publish multi-field updates with an odd/even Seq.
	maps     [][]byte
	bss      []byte
	cpuShard []uint32
	nrShards int
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
	return &Sched{handle: h, nrShards: 1}, nil
}

// SetUserschedTgid must precede Load; it exempts the Go process from external dispatch.
func (s *Sched) SetUserschedTgid(tgid uint32) error {
	return check(C.ge_set_tgid(s.handle, C.uint32_t(tgid)))
}

// SetShards configures CPU-to-shard ownership before Load. Unspecified CPUs
// belong to shard zero; each configured CPU must have a valid shard ID.
func (s *Sched) SetShards(cpuShard []uint32, nr uint32) error {
	if nr == 0 || nr > MaxShards || len(cpuShard) > MaxCPUs {
		return syscall.EINVAL
	}
	for _, id := range cpuShard {
		if id >= nr {
			return syscall.EINVAL
		}
	}
	var ptr *C.uint32_t
	if len(cpuShard) != 0 {
		ptr = (*C.uint32_t)(unsafe.Pointer(&cpuShard[0]))
	}
	if err := check(C.ge_set_shards(s.handle, ptr, C.uint32_t(len(cpuShard)), C.uint32_t(nr))); err != nil {
		return err
	}
	s.cpuShard = append([]uint32(nil), cpuShard...)
	s.nrShards = int(nr)
	return nil
}

// RegisterUsersched assigns a Linux thread ID (not TGID) to its scheduler shard.
// Call after Load and before the thread becomes runnable in sched_ext.
func (s *Sched) RegisterUsersched(tid uint32, shard uint32) error {
	return check(C.ge_register_usersched(s.handle, C.uint32_t(tid), C.uint32_t(shard)))
}

// DisableOp disables an optional struct_ops callback before Load.
func (s *Sched) DisableOp(name string) error {
	cname := C.CString(name)
	defer C.free(unsafe.Pointer(cname))
	return check(C.ge_disable_op(s.handle, cname))
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
	return s.ConsumeShard(0, out)
}

// ConsumeShard reads at most one event from this shard without blocking.
func (s *Sched) ConsumeShard(id int, out *Ev) (bool, error) {
	if id < 0 || id >= s.nrShards {
		return false, syscall.EINVAL
	}
	code := C.ge_consume_shard(s.handle, C.uint32_t(id), (*C.struct_ev)(unsafe.Pointer(out)))
	runtime.KeepAlive(s)
	if code < 0 {
		return false, check(code)
	}
	return code == 1, nil
}

// Submit queues dispatch/control commands and kicks their target CPU.
// Hrtick commands execute immediately through a syscall program so a running
// CPU need not be preempted just to arm its timer. ENOSPC means the user ring
// is full. A kick failure does not invalidate a queued command.
func (s *Sched) Submit(c *Cmd) error {
	return s.SubmitShard(s.ShardForCPU(c.Cpu), c)
}

// ShardForCPU reports configured ownership; unknown CPUs belong to shard 0.
func (s *Sched) ShardForCPU(cpu int32) int {
	if cpu >= 0 && int(cpu) < len(s.cpuShard) {
		return int(s.cpuShard[cpu])
	}
	return 0
}

// SubmitShard rejects commands targeting CPUs outside this shard.
func (s *Sched) SubmitShard(id int, c *Cmd) error {
	if id < 0 || id >= s.nrShards || c.Cpu < 0 || c.Cpu >= MaxCPUs ||
		s.ShardForCPU(c.Cpu) != id {
		return syscall.EINVAL
	}
	if c.Kind == CmdHrtick {
		return check(C.ge_hrtick(s.handle, C.int32_t(c.Cpu), C.uint32_t(c.Slot), C.uint64_t(c.Slice)))
	}
	code := C.ge_submit_shard(s.handle, C.uint32_t(id), (*C.struct_cmd)(unsafe.Pointer(c)))
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
	s.DecNrQueuedShard(0, n)
}
func (s *Sched) shardCounter(name string, id int) *uint64 {
	if id < 0 || id >= s.nrShards {
		return nil
	}
	p := s.counter(name)
	if p == nil || int(uintptr(unsafe.Pointer(p))-uintptr(unsafe.Pointer(&s.bss[0])))+8*(id+1) > len(s.bss) {
		return nil
	}
	return (*uint64)(unsafe.Add(unsafe.Pointer(p), id*8))
}
func decCounter(p *uint64, n uint64) {
	if p == nil {
		return
	}
	for {
		old := atomic.LoadUint64(p)
		if atomic.CompareAndSwapUint64(p, old, old-min(old, n)) {
			return
		}
	}
}
func (s *Sched) DecNrQueuedShard(id int, n uint64) {
	decCounter(s.shardCounter("nr_queued_s", id), n)
	if id < 0 || id >= s.nrShards {
		return
	}
	decCounter(s.counter("nr_queued"), n)
}
func (s *Sched) SetNrScheduledShard(id int, n uint64) {
	if p := s.shardCounter("nr_scheduled_s", id); p != nil {
		old := atomic.SwapUint64(p, n)
		if total := s.counter("nr_scheduled"); total != nil {
			atomic.AddUint64(total, n-old)
		}
	}
}
func (s *Sched) SetNeededShard(id int) {
	if p := s.shardCounter("needed_s", id); p != nil {
		atomic.StoreUint64(p, 1)
	}
}
func (s *Sched) NrQueuedShard(id int) uint64 {
	if p := s.shardCounter("nr_queued_s", id); p != nil {
		return atomic.LoadUint64(p)
	}
	return 0
}
func (s *Sched) SetNrScheduled(n uint64) {
	s.SetNrScheduledShard(0, n)
}

// Publish updates a task's shared core-scheduling and debug fields. The
// scheduler goroutine is the sole writer; slot zero is reserved.
func (s *Sched) Publish(slot uint32, pub TaskPub) {
	if slot == 0 || int(slot) >= len(s.Task) {
		return
	}
	dst := &s.Task[slot]
	seq := atomic.LoadUint64(&dst.Seq)
	atomic.StoreUint64(&dst.Seq, seq+1)
	dst.Pid = pub.Pid
	dst.CoreVtime = pub.CoreVtime
	dst.Vruntime = pub.Vruntime
	dst.Deadline = pub.Deadline
	dst.Lag = pub.Lag
	dst.Weight = pub.Weight
	dst.PublishedAt = 0
	if pub.Pid != 0 {
		// Charging may precede publication, so starting projection at the
		// publication clock prevents counting that charged interval twice.
		var ts syscall.Timespec
		_, _, errno := syscall.RawSyscall(syscall.SYS_CLOCK_GETTIME, 1, uintptr(unsafe.Pointer(&ts)), 0)
		dst.PublishedAt = ^uint64(0)
		if errno == 0 && ts.Sec >= 0 && ts.Nsec >= 0 {
			dst.PublishedAt = uint64(ts.Sec)*1e9 + uint64(ts.Nsec)
		}
	}
	atomic.StoreUint64(&dst.Seq, seq+2)
}

func (s *Sched) RunSeq(cpu int32) uint64 {
	if cpu < 0 || int(cpu) >= len(s.CPU) {
		return 0
	}
	return atomic.LoadUint64(&s.CPU[cpu].RunSeq)
}

// SetPerf publishes a scheduler performance target (1..1024); zero disables
// tick-based updates on that CPU.
func (s *Sched) SetPerf(cpu int32, perf uint64) {
	if cpu < 0 || int(cpu) >= len(s.Ctl) {
		return
	}
	if perf > 1024 {
		perf = 1024
	}
	atomic.StoreUint64(&s.Ctl[cpu].PerfTarget, perf)
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
