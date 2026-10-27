package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"runtime"
	"strconv"
	"strings"
	"sync/atomic"
	"syscall"
	"time"
	"unsafe"

	"github.com/Gthulhu/scx_goeevdf/eevdf"
	"github.com/Gthulhu/scx_goeevdf/internal/bpf"
)

type backend struct {
	sched    *bpf.Sched
	pressure *eevdf.Pressure
}

func (b backend) Submit(c eevdf.Cmd) error {
	cmd := bpf.Cmd{
		Kind: c.Kind, Pid: c.Pid, Cpu: c.Cpu, Cflags: c.Cflags,
		Slice: c.Slice, EnqFlags: c.EnqFlags, EnqSeq: c.EnqSeq,
		Slot: c.Slot, ExpectCurr: c.ExpectCurr, KickFlags: c.KickFlags,
	}
	return b.sched.Submit(&cmd)
}

func (b backend) Idle(cpu int32) bool {
	return atomic.LoadUint32(&b.sched.CPU[cpu].Idle) != 0
}

func (b backend) CurrPid(cpu int32) int32 {
	return int32(atomic.LoadUint32(&b.sched.CPU[cpu].CurrPid))
}

func (b backend) RunSeq(cpu int32) uint64 {
	return atomic.LoadUint64(&b.sched.CPU[cpu].RunSeq)
}

func (b backend) Available(cpu int32) uint64 {
	return b.pressure.Available(cpu)
}

func (b backend) SetQueued(cpu int32, n, keepSlice uint64) {
	atomic.StoreUint64(&b.sched.Ctl[cpu].NrQueued, n)
	atomic.StoreUint64(&b.sched.Ctl[cpu].KeepSlice, keepSlice)
}

func (b backend) Affinity(pid int32) ([]int32, error) {
	var mask [128]byte // MAX_CPUS = 1024
	_, _, errno := syscall.RawSyscall(syscall.SYS_SCHED_GETAFFINITY,
		uintptr(pid), uintptr(len(mask)), uintptr(unsafe.Pointer(&mask[0])))
	if errno != 0 {
		return nil, errno
	}
	cpus := make([]int32, 0, 8)
	for i, bits := range mask {
		for bit := 0; bit < 8; bit++ {
			if bits&(1<<bit) != 0 {
				cpus = append(cpus, int32(i*8+bit))
			}
		}
	}
	return cpus, nil
}

func possibleCPUs() (int, error) {
	data, err := os.ReadFile("/sys/devices/system/cpu/possible")
	if err != nil {
		return 0, err
	}
	maxCPU := -1
	for _, span := range strings.Split(strings.TrimSpace(string(data)), ",") {
		parts := strings.Split(span, "-")
		n, err := strconv.Atoi(parts[len(parts)-1])
		if err != nil || n < 0 {
			return 0, fmt.Errorf("invalid possible CPU range %q", span)
		}
		maxCPU = max(maxCPU, n)
	}
	if maxCPU >= bpf.MaxCPUs {
		return 0, fmt.Errorf("kernel has %d possible CPUs, limit is %d", maxCPU+1, bpf.MaxCPUs)
	}
	return maxCPU + 1, nil
}

func clockNow() uint64 {
	var ts syscall.Timespec
	_, _, errno := syscall.RawSyscall(syscall.SYS_CLOCK_GETTIME, 1,
		uintptr(unsafe.Pointer(&ts)), 0)
	if errno != 0 {
		return 0
	}
	return uint64(ts.Sec)*1_000_000_000 + uint64(ts.Nsec)
}

func run(ctx context.Context, object string) error {
	ncpu, err := possibleCPUs()
	if err != nil {
		return err
	}
	runtime.LockOSThread()
	defer runtime.UnlockOSThread()
	s, err := bpf.OpenPath(object)
	if err != nil {
		return err
	}
	defer s.Close()
	if err := s.SetUserschedTgid(uint32(os.Getpid())); err != nil {
		return err
	}
	if err := s.Load(); err != nil {
		return err
	}
	pressure := eevdf.NewPressure(ncpu)
	p := eevdf.New(backend{sched: s, pressure: pressure}, ncpu, eevdf.Enums{
		EnqWakeup: 1, EnqPreempt: 1 << 32,
	})
	if err := p.SetTopology(readTopology("/sys/devices/system/cpu", ncpu)); err != nil {
		return fmt.Errorf("CPU topology: %w", err)
	}
	var groups groupSync
	samples := make([]eevdf.StatSample, ncpu)
	if err := s.Attach(); err != nil {
		return err
	}
	nextTick := clockNow()
	nextPressure := nextTick
	drops := s.Counter("nr_ev_drops")
	recoverUntil := uint64(0)
	for ctx.Err() == nil {
		count := 0
		for ; count < 512; count++ {
			var ev bpf.Ev
			ok, err := s.Consume(&ev)
			if err != nil {
				return err
			}
			if !ok {
				break
			}
			if ev.Kind == eevdf.EvEnqueue {
				s.DecNrQueued(1)
				groups.sync(p, ev.Pid, ev.Cgid)
			} else if ev.Kind == bpf.EvCgrpExit {
				p.RemoveCgroup(ev.Cgid)
				delete(groups.last, ev.Cgid)
				delete(groups.applied, ev.Cgid)
				delete(groups.paths, ev.Cgid)
			} else if ev.Kind == bpf.EvCgrpWeight || ev.Kind == bpf.EvCgrpBW {
				groups.refresh(p, ev.Cgid)
			}
			p.Handle(eevdf.Event{
				Kind: ev.Kind, Pid: ev.Pid, Tgid: ev.Tgid, Cpu: ev.Cpu,
				Flags: ev.Flags, Now: ev.Now, SumExec: ev.SumExec,
				Weight: ev.Weight, CustomSlice: ev.CustomSlice,
				WakerPid: ev.WakerPid, WakerCpu: ev.WakerCpu,
				WakeFlags: ev.WakeFlags, Policy: ev.Policy, EnqSeq: ev.EnqSeq,
				Cgid: ev.Cgid,
			})
		}
		p.Schedule()
		if now := clockNow(); now >= nextTick {
			p.Periodic(now)
			nextTick = now + 1_000_000
			if current := s.Counter("nr_ev_drops"); current != drops {
				drops = current
				recoverUntil = now + 1_000_000_000
			}
			if now < recoverUntil {
				p.ReconcileDropped()
				p.Schedule()
			}
			if now >= nextPressure {
				if data, err := os.ReadFile("/proc/stat"); err == nil {
					if eevdf.ParseProcStat(data, samples, 100) == nil {
						for cpu := 0; cpu < ncpu; cpu++ {
							c := int32(cpu)
							demand := atomic.LoadUint32(&s.CPU[cpu].CurrPid) != 0 ||
								atomic.LoadUint64(&s.Ctl[cpu].NrQueued) != 0 ||
								atomic.LoadUint64(&s.CPU[cpu].ReleasedAt) != 0
							pressure.Update(c, now, demand,
								atomic.LoadUint64(&s.CPU[cpu].DisplacedNs), samples[cpu])
						}
					}
				}
				nextPressure = now + 32_000_000
			}
		}
		if p.Pending() {
			s.SetNrScheduled(1)
		} else {
			s.SetNrScheduled(0)
		}
		if count == 0 {
			time.Sleep(100 * time.Microsecond)
		}
	}
	return nil
}

func main() {
	object := flag.String("bpf-object", "bpf/goeevdf.bpf.o", "compiled sched_ext BPF object")
	flag.Parse()
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if err := run(ctx, *object); err != nil && !errors.Is(err, context.Canceled) {
		fmt.Fprintln(os.Stderr, "goeevdf:", err)
		os.Exit(1)
	}
}
