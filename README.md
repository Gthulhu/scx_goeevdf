# scx_goeevdf

[English](./README.md) | [繁體中文](./README.zh.md)

`scx_goeevdf` is a Linux [`sched_ext`](https://www.kernel.org/doc/html/latest/scheduler/sched-ext.html) user-space CPU scheduler written in Go, implementing the complete **EEVDF (Earliest Eligible Virtual Deadline First)** algorithm from Linux's `fair.c` (modeled on [`scx_reevdf`](https://github.com/sched-ext/scx/pull/3926) and built on the eBPF/cgo transport patterns from [`Gthulhu/qumun`](https://github.com/Gthulhu/qumun)).

All scheduling policies, virtual runtime accounting, augmented eligible-deadline trees, topology-aware placement, load balancing, cgroup hierarchy weights, `cpu.max` bandwidth control, capacity pressure, and cpufreq scaling live in pure, unit-tested Go (`eevdf/`). The kernel eBPF program (`bpf/goeevdf.bpf.c`) serves as a thin, non-blocking data plane that streams lifecycle events to Go, executes dispatch commands, and handles synchronous kernel fast paths.

---

## Architecture: How eBPF Interacts with the Go Application

![eBPF and Go Scheduler Interaction (Manim Animation)](./docs/assets/ebpf_go_interaction.gif)

<details>
<summary>View Static High-Resolution Diagram (PNG)</summary>

![eBPF and Go Scheduler Interaction (Static)](./docs/assets/ebpf_go_interaction.png)

</details>

> The visualization above is rendered with [Manim](./docs/manim/ebpf_go_interaction.py).

### Four Communication Channels Across the Kernel / User-Space Boundary

`scx_goeevdf` decouples kernel mechanisms from user-space scheduling decisions through four zero-copy or non-blocking channels:

| # | Channel | Direction | eBPF Side (`bpf/goeevdf.bpf.c`) | Go Side (`internal/bpf` & `cmd/goeevdf`) | Purpose |
|---|---|---|---|---|---|
| **1** | **Event Ring Buffer** (`BPF_MAP_TYPE_RINGBUF` inside `ev_rings` `ARRAY_OF_MAPS`) | **eBPF → Go** | `ev_reserve()` + `bpf_ringbuf_submit()` writes 128-byte `struct ev` records (`EV_ENQUEUE`, `EV_SLEEP`, `EV_DEQUEUE`, `EV_YIELD`, `EV_IDLE`, `EV_CPUMASK`, `EV_CGRP_*`) | `Sched.ConsumeShard(s, &ev)` drains events one by one without blocking (`ring_buffer__consume`) | Streams task wakeups, slice expirations, sleep/exit transitions, idle CPU notifications, and cgroup updates to the owning Go shard |
| **2** | **Shared Memory Tables** (`BPF_F_MMAPABLE` arrays + `.bss` + `.rodata`) | **Bidirectional (Zero-Copy)** | Reads/writes `cpu_tbl`, `ctl_tbl`, `task_tbl`, and `.bss` counters (`nr_queued_s`, `nr_scheduled_s`, `nr_ev_drops`) | Direct `syscall.Mmap` slices (`s.CPU`, `s.Ctl`, `s.Task`, `s.bss`) accessed via `sync/atomic` | Zero-syscall state sharing: BPF reports running PID, `run_seq`, idle state, and RT/DL displacement (`cpu_tbl`); Go publishes `nr_queued`, `keep_slice`, `perf_target` (`ctl_tbl`), and sequence-locked task vruntime/deadline (`task_tbl`) |
| **3** | **Command User Ring Buffer** (`BPF_MAP_TYPE_USER_RINGBUF`, `cmds_0..31`) | **Go → eBPF** | `drain_shard()` calls `bpf_user_ringbuf_drain(&cmds_s, apply_cmd, ...)` inside `ge_dispatch` to insert tasks into per-CPU DSQs (`DSQ_BASE + cpu`) or stop running tasks | `Sched.SubmitShard(s, &cmd)` reserves & submits 64-byte `struct cmd` records (`CMD_DISPATCH`, `CMD_STOP`, `CMD_KICK`) | Sends Go's EEVDF scheduling decisions (`pid`, `cpu`, `slice`, `enq_seq`, `slot`, `CMDF_HRTICK`) back to the kernel |
| **4** | **Syscall Programs** (`SEC("syscall")` via `BPF_PROG_TEST_RUN`) | **Go → eBPF** | `ge_kick_prog` invokes `scx_bpf_kick_cpu(cpu, flags)`; `ge_hrtick_prog` arms per-CPU `bpf_timer` via `hrtick_arm_at()` | `Sched.Kick(cpu, flags)` and `Sched.Hrtick(cpu, runSeq, when)` | Immediately wakes or preempts target CPUs after submitting commands, or arms high-resolution preemption timers without waiting for a reschedule |

### Closed-Loop Execution Lifecycle

1. **Scheduler Exemption & Self-Scheduling**: Before `Load()`, Go writes its process TGID (`usersched_tgid`) and CPU-to-shard ownership (`cpu_shard[]`, `nr_shards`) into `.rodata`. Each Go scheduler OS thread (`runtime.LockOSThread()`) registers its Linux TID in `usersched_tids`. When a scheduler thread is enqueued, eBPF bypasses user-space rings and dispatches it directly to `SCHED_DSQ_BASE + shard` so the scheduler never deadlocks waiting for itself.
2. **Task Enqueue & Idle Fast Path**: When a normal task enters `ge_enqueue`:
   - If the target CPU is idle and the shard has no backlog (`nr_queued_s[shard] == 0` and `nr_scheduled_s[shard] == 0`), eBPF dispatches the task directly to `SCX_DSQ_LOCAL` and still emits `EV_ENQUEUE` so Go's EEVDF pack stays synchronized.
   - Otherwise, eBPF marks the task as `waiting = true`, increments `nr_queued_s[shard]`, emits `EV_ENQUEUE` to `ev_rings[shard]`, and wakes the shard's scheduler thread if needed.
3. **Go EEVDF Policy Evaluation**: The Go loop consumes up to 512 events per batch, updates the per-CPU EEVDF `Pack` and augmented eligible-deadline tree (`eevdf/tree.go`), computes virtual deadlines and wakeup preemption, and updates `ctl_tbl[cpu]` (`nr_queued`, `keep_slice`) and `task_tbl[slot]` (`core_vtime`, `vruntime`, `deadline`, `lag`).
4. **Command Dispatch & Kernel Drain**: Go emits `CMD_DISPATCH` into `cmds_s` and triggers `ge_kick_prog` when immediate preemption or idle wakeup is needed. On the target CPU, `ge_dispatch` drains `cmds_s`, validates `t->waiting && t->enq_seq == c->enq_seq` (dropping stale commands safely), inserts the task into `DSQ_BASE + c->cpu`, and moves it to the local runqueue via `scx_bpf_dsq_move_to_local()`.
5. **Synchronous Kernel Collaborators**:
   - **Slice Continuation (`keep_running`)**: When a task's slice expires in `ge_stopping`/`ge_tick` and Go has pre-authorized continuation via `ctl_tbl[cpu].keep_slice`, eBPF refills `p->scx.slice` in-kernel and emits `EV_KEEP` without a round-trip stall.
   - **High-Resolution Preemption (`hrtick`)**: If `CMDF_HRTICK` is set on a dispatch command or triggered via `ge_hrtick_prog`, eBPF arms a per-CPU `bpf_timer` (`BPF_F_TIMER_ABS`) that fires `scx_bpf_kick_cpu(cpu, SCX_KICK_PREEMPT)` at the exact preemption deadline.
   - **Core Scheduling & Task Dump**: `ge_core_sched_before` and `ge_dump_task` read Go's sequence-locked `task_tbl[slot]` directly in kernel context.

---

## Key Features

- **Full EEVDF Accounting (`eevdf/accounting.go`, `eevdf/pack.go`, `eevdf/tree.go`)**:
  - Per-CPU `Pack` maintaining weighted reference virtual time $V$ with remainder preservation.
  - Augmented red-black/eligible-deadline tree supporting $O(\log N)$ lookup of the earliest-deadline eligible task (`min_vruntime` subtree pruning).
  - Lag clamping, `PLACE_LAG`, `PLACE_REL_DEADLINE`, `RUN_TO_PARITY` slice protection, `PREEMPT_SHORT`, and `DELAY_DEQUEUE` / `DELAY_REQUEUE` debt repayment.
- **Topology-Aware Placement & Three-Stage Load Balancing (`eevdf/placement.go`, `eevdf/balance.go`, `eevdf/newidle.go`, `eevdf/active.go`)**:
  - Wakeup CPU selection across SMT core, LLC, NUMA node, and system hierarchy (`WAKE_SYNC`, `WA_WEIGHT`, `SIS_UTIL`).
  - Newly-idle pull (`newidle`), periodic load balancing, and active migration (`CMD_STOP` + remote re-dispatch).
- **Cgroup v2 Weights & `cpu.max` Bandwidth Control (`eevdf/cgroup.go`, `eevdf/bandwidth.go`)**:
  - Hierarchical task-group effective weight folding (`cpu.weight`).
  - Quota/period/burst token bucket enforcement (`cpu.max`) with throttling, unthrottling, and shared quota pools.
- **Capacity Pressure & CPU Frequency Scaling (`eevdf/pressure.go`, `eevdf/cpufreq.go`)**:
  - Tracks RT/DL/IRQ/steal capacity displacement via `cpu_release`/`cpu_acquire` and `/proc/stat`.
  - Utilization-driven performance target updates applied via `scx_bpf_cpuperf_set`.
- **Per-LLC Sharding (`internal/shard/`, `cmd/goeevdf/sharded.go`)**:
  - Automatic per-LLC sharding on systems with $>64$ possible CPUs (`-shards auto`), isolating per-shard rings, OS threads, and EEVDF policies with lock-free cross-shard snapshots and explicit task handoffs.

---

## Repository Layout

| Path | Description |
|---|---|
| [`bpf/intf.h`](./bpf/intf.h) | Shared C/Go wire definitions (`struct ev`, `struct cmd`, `struct cpu_slot`, `struct cpu_ctl`, `struct task_pub`) |
| [`bpf/goeevdf.bpf.c`](./bpf/goeevdf.bpf.c) | `sched_ext` `struct_ops` eBPF program, per-shard ring buffers, mmapable tables, `hrtick` timers, and syscall programs |
| [`internal/bpf/`](./internal/bpf) | `libbpf` C bridge (`bridge.c`, `bridge.h`) and Go wrapper (`sched.go`, `wire.go`) |
| [`eevdf/`](./eevdf) | Pure Go EEVDF scheduling engine (accounting, pack, tree, placement, preemption, balance, cgroup, bandwidth, pressure, cpufreq, hrtick, coresched) |
| [`internal/shard/`](./internal/shard) | Multi-LLC shard coordinator, task ownership routing, and cross-shard handoff/steal engine |
| [`cmd/goeevdf/`](./cmd/goeevdf) | CLI entrypoint, sysfs topology/cgroup readers, single-loop and sharded runtime loops |
| [`docs/manim/`](./docs/manim) | Manim scene script generating the eBPF ↔ Go interaction diagrams |

---

## Building and Running

### Prerequisites

- **Linux Kernel**: 6.12+ with `CONFIG_SCHED_CLASS_EXT=y` (`sched_ext`) and BTF enabled (`/sys/kernel/btf/vmlinux`)
- **Build Toolchain**:
  - Go 1.24+
  - `clang` (with BPF target support)
  - `bpftool`
  - `libbpf` (`libbpf.so.1` and headers), `libelf`, `zlib`

### Build

```bash
make build
```

This generates `bpf/vmlinux.h` from the running kernel BTF, compiles `bpf/goeevdf.bpf.o`, and builds the user-space binary at `bin/goeevdf`.

### Run Tests

```bash
# Run all unit and race/simulation tests (no root required)
go test ./...

# Run privileged BPF load/ringbuf integration tests (requires root / CAP_BPF)
sudo GOEEVDF_TEST_BPF_LOAD=1 go test -v ./internal/bpf
```

### Run the Scheduler

Running a `sched_ext` scheduler requires `root` (or `CAP_BPF` + `CAP_SYS_ADMIN`):

```bash
sudo ./bin/goeevdf
```

#### Common CLI Flags

```text
  -bpf-object string
        compiled sched_ext BPF object (default "bpf/goeevdf.bpf.o")
  -shards string
        per-LLC sharding above 64 possible CPUs: "auto" or "1" (default "auto")
  -no-wakeup-preempt
        disable wakeup preemption
  -no-eligibility
        disable EEVDF eligibility
  -no-eligible-scan
        disable eligible-tree scan
  -no-run-to-parity
        disable run-to-parity protection
  -no-preempt-short
        disable short-request preemption
  -no-place-lag
        disable placement lag
  -no-place-rel-deadline
        disable relative-deadline placement
  -no-delay-dequeue
        disable deferred dequeue
  -no-delay-requeue
        disable deferred requeue
  -no-proxy-exec
        disable blocked donor handling
  -no-hrtick
        disable hrtick preemption
  -no-sis-util
        disable utilization-aware idle selection
  -no-newidle-cost
        disable idle search cost limit
  -no-newidle-sampling
        disable idle search sampling
  -no-capacity-pressure
        disable capacity pressure
  -disable-cgroups
        disable cgroup weights and quotas
  -disable-cpu-max
        disable cpu.max enforcement
  -disable-cpufreq
        disable CPU performance updates
```

---

## Re-rendering the Manim Diagrams

To regenerate the architecture PNG and animated GIF in `docs/assets/`:

```bash
# Render high-resolution static PNG
manim -qh -s docs/manim/ebpf_go_interaction.py EbpfGoInteraction

# Render video and convert to palette-optimized GIF
manim -qm docs/manim/ebpf_go_interaction.py EbpfGoInteraction
```
