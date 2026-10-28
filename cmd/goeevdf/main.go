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

func (b backend) RunStart(cpu int32) uint64 {
	return atomic.LoadUint64(&b.sched.CPU[cpu].RunStart)
}

func (b backend) Publish(slot uint32, value eevdf.TaskPub) {
	b.sched.Publish(slot, bpf.TaskPub{
		Pid: value.Pid, CoreVtime: value.CoreVtime,
		Vruntime: value.Vruntime, Deadline: value.Deadline,
		Lag: value.Lag, Weight: value.Weight,
	})
}

func (b backend) SetPerf(cpu int32, perf uint64) {
	b.sched.SetPerf(cpu, perf)
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

type perfIO interface{ SetPerf(cpu int32, perf uint64) }

func updateFrequency(p *eevdf.Policy, gov *eevdf.FrequencyGovernor, io perfIO, now uint64, cpus int, attached map[int32]bool) {
	if !p.F.Cpufreq {
		return
	}
	for c := 0; c < cpus; c++ {
		cpu := int32(c)
		util, capacity, runnable := p.FrequencySample(cpu, now)
		if target, changed := gov.Update(cpu, now, util, capacity, runnable, attached[cpu]); changed {
			io.SetPerf(cpu, target)
		}
	}
}

func run(ctx context.Context, object string, features eevdf.Features) error {
	enums, features, err := runtimeEnums(features)
	if err != nil {
		return err
	}
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
	s.SetKickFlags(enums.KickIdle, enums.KickPreempt)
	if err := s.SetUserschedTgid(uint32(os.Getpid())); err != nil {
		return err
	}
	allowed, err := ioAffinity()
	if err != nil {
		return err
	}
	if err := s.SetUserschedCPUs([]uint32{uint32(allowed[0])}); err != nil {
		return err
	}
	if err := s.Load(); err != nil {
		return err
	}
	tid, _, errno := syscall.RawSyscall(syscall.SYS_GETTID, 0, 0, 0)
	if errno != 0 {
		return errno
	}
	if err := s.RegisterUsersched(uint32(tid), 0); err != nil {
		return err
	}
	pressure := eevdf.NewPressure(ncpu)
	io := backend{sched: s, pressure: pressure}
	p := eevdf.New(io, ncpu, policyEnums(enums))
	p.F = features
	p.F.Normalize()
	if err := p.SetTopology(readTopology("/sys/devices/system/cpu", ncpu)); err != nil {
		return fmt.Errorf("CPU topology: %w", err)
	}
	var groups groupSync
	samples := make([]eevdf.StatSample, ncpu)
	governor := eevdf.NewFrequencyGovernor(ncpu, 10_000_000)
	if err := s.Attach(); err != nil {
		return err
	}
	nextTick := clockNow()
	nextPressure := nextTick
	drops := s.Counter("nr_ev_drops")
	var gc taskGC
	recoverUntil := uint64(0)
	for ctx.Err() == nil {
		count := 0
		attached := make(map[int32]bool)
		for ; count < 512; count++ {
			var ev bpf.Ev
			ok, err := s.Consume(&ev)
			if err != nil {
				return err
			}
			if !ok {
				break
			}
			gc.observe(ev)
			if ev.Kind == eevdf.EvEnqueue {
				s.DecNrQueued(1)
				if err := groups.sync(p, ev.Pid, ev.Cgid); err != nil {
					return fmt.Errorf("enqueue cgroup %d: %w", ev.Cgid, err)
				}
			} else if ev.Kind == bpf.EvCgrpExit {
				p.RemoveCgroup(ev.Cgid)
				delete(groups.last, ev.Cgid)
				delete(groups.applied, ev.Cgid)
				delete(groups.paths, ev.Cgid)
			} else if ev.Kind == bpf.EvCgrpWeight || ev.Kind == bpf.EvCgrpIdle || ev.Kind == bpf.EvCgrpBW {
				if err := groups.refresh(p, ev.Cgid); err != nil {
					return fmt.Errorf("refresh cgroup %d: %w", ev.Cgid, err)
				}
			}
			p.Handle(eevdf.Event{
				Kind: ev.Kind, Pid: ev.Pid, Tgid: ev.Tgid, Cpu: ev.Cpu,
				Flags: ev.Flags, Now: ev.Now, SumExec: ev.SumExec,
				Weight: ev.Weight, CustomSlice: ev.CustomSlice,
				WakerPid: ev.WakerPid, WakerCpu: ev.WakerCpu,
				WakeFlags: ev.WakeFlags, Policy: ev.Policy, EnqSeq: ev.EnqSeq,
				Cgid: ev.Cgid,
			})
			if ev.Kind == eevdf.EvEnqueue {
				if cpu, ok := p.TaskCPU(ev.Pid); ok {
					attached[cpu] = true
				}
			}
		}
		p.Schedule()
		if len(attached) != 0 {
			updateFrequency(p, governor, io, clockNow(), ncpu, attached)
		}
		if now := clockNow(); now >= nextTick {
			if kind := s.Counter("exit_kind"); kind != 0 {
				return fmt.Errorf("kernel scheduler exited: kind=%d code=%d", kind, int64(s.Counter("exit_code")))
			}
			if err := gc.collect(now, s.Counter("nr_ev_drops"),
				func(pid int32) error { return syscall.Kill(int(pid), 0) },
				func(ev bpf.Ev) error {
					p.Handle(eevdf.Event{Kind: eevdf.EvExit, Pid: ev.Pid,
						Cpu: ev.Cpu, SumExec: ev.SumExec, Now: ev.Now})
					return nil
				}); err != nil {
				return err
			}
			p.Periodic(now)
			p.BalanceTick(now)
			updateFrequency(p, governor, io, now, ncpu, nil)
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
	shards := flag.String("shards", "auto", "per-LLC sharding above 64 possible CPUs (auto or 1)")
	noWakePreempt := flag.Bool("no-wakeup-preempt", false, "disable wakeup preemption")
	noEligibility := flag.Bool("no-eligibility", false, "disable EEVDF eligibility")
	noEligibleScan := flag.Bool("no-eligible-scan", false, "disable eligible-tree scan")
	noRunToParity := flag.Bool("no-run-to-parity", false, "disable run-to-parity protection")
	noPreemptShort := flag.Bool("no-preempt-short", false, "disable short-request preemption")
	noPlaceLag := flag.Bool("no-place-lag", false, "disable placement lag")
	noPlaceDeadline := flag.Bool("no-place-rel-deadline", false, "disable relative-deadline placement")
	noDelayDequeue := flag.Bool("no-delay-dequeue", false, "disable deferred dequeue")
	noDelayRequeue := flag.Bool("no-delay-requeue", false, "disable deferred requeue")
	noProxyExec := flag.Bool("no-proxy-exec", false, "disable blocked donor handling")
	noHrtick := flag.Bool("no-hrtick", false, "disable hrtick preemption")
	noVrefUpdate := flag.Bool("no-vref-update", false, "disable running-task reference-time projection")
	noWakeSync := flag.Bool("no-wake-sync", false, "disable synchronous wake-affine placement")
	noWaWeight := flag.Bool("no-wa-weight", false, "disable weighted wake-affine placement")
	noSISUtil := flag.Bool("no-sis-util", false, "disable utilization-aware idle selection")
	noNewidleCost := flag.Bool("no-newidle-cost", false, "disable idle search cost limit")
	noNewidleSampling := flag.Bool("no-newidle-sampling", false, "disable idle search sampling")
	noCapacityPressure := flag.Bool("no-capacity-pressure", false, "disable capacity pressure")
	disableCgroups := flag.Bool("disable-cgroups", false, "disable cgroup weights and quotas")
	disableCPUQuota := flag.Bool("disable-cpu-max", false, "disable cpu.max enforcement")
	disableCpufreq := flag.Bool("disable-cpufreq", false, "disable CPU performance updates")
	flag.Parse()
	features := eevdf.DefaultFeatures()
	features.WakeupPreempt = !*noWakePreempt
	features.Eligibility = !*noEligibility
	features.EligibleScan = !*noEligibleScan
	features.RunToParity = !*noRunToParity
	features.PreemptShort = !*noPreemptShort
	features.PlaceLag = !*noPlaceLag
	features.PlaceRelDeadline = !*noPlaceDeadline
	features.DelayDequeue = !*noDelayDequeue
	features.DelayRequeue = !*noDelayRequeue
	features.ProxyExec = !*noProxyExec
	features.Hrtick = !*noHrtick
	features.VrefUpdate = !*noVrefUpdate
	features.WakeSync = !*noWakeSync
	features.WaWeight = !*noWaWeight
	features.SISUtil = !*noSISUtil
	features.NewidleCost = !*noNewidleCost
	features.NewidleSampling = !*noNewidleSampling
	features.CapacityPressure = !*noCapacityPressure
	features.Cgroups = !*disableCgroups
	features.CpuMax = !*disableCPUQuota
	features.Cpufreq = !*disableCpufreq
	if err := prepareRuntime(); err != nil {
		fmt.Fprintln(os.Stderr, "goeevdf:", err)
		os.Exit(1)
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	ncpu, err := possibleCPUs()
	if err == nil {
		topo := readTopology("/sys/devices/system/cpu", ncpu)
		var groups [][]int32
		groups, err = selectShardGroups(ncpu, topo, *shards)
		if err == nil {
			if groups == nil {
				err = run(ctx, *object, features)
			} else {
				err = runSharded(ctx, *object, features, topo, groups)
			}
		}
	}
	if err != nil && !errors.Is(err, context.Canceled) {
		fmt.Fprintln(os.Stderr, "goeevdf:", err)
		os.Exit(1)
	}
}
