# Plan alignment implementation tasks

Source of requirements: [plan.md](./plan.md). Status is evidence-based:
`pending`, `in progress`, `implemented` (targeted tests pass), or `verified`
(the stated integration acceptance also passes). Do not infer completion from
the presence of a module or from passing mocked tests.

## Execution order and ownership

Independent workstreams may run concurrently, but each must own its listed
files. Integration changes require checking all single-loop and sharded paths.
Do not change scheduling semantics merely to make an existing test pass.

### A1. Kernel enums and proxy execution — implemented; proxy VM acceptance pending

- Scope: `internal/bpf`, BPF configuration, both runtime constructors.
- Import scheduling flags from the running kernel's BTF, rather than hardcoding enum
  values. Missing optional blocked-enqueue support must explicitly disable
  proxy execution; missing required enum values must fail startup.
- Wire `EnqBlocked` into single-loop and shard policies.
- Acceptance: unit tests for required/optional enum availability, blocked
  donors retain their CPU/request, and both startup paths use imported enums.
- Depends on: none.
- Evidence: required/optional enum tests and runtime conversion tests pass;
  single-loop and shard constructors use the same conversion. VM load confirms
  explicit proxy disable on `6.14.0-37-generic` (no `SCX_ENQ_BLOCKED`).
  A blocked-enqueue-capable kernel is still needed for proxy integration tests.

### A2. Placement and effective feature switches — in progress

- Scope: `eevdf/placement.go`, task wake state, preemption/accounting projection,
  feature switches and related Go tests.
- Connect wake-wide, wake-affine, fork placement, and wake flags to actual
  selection; preserve affinity, delayed debt, and blocked-donor restrictions.
- Connect reference-time projection and the currently unused feature switches.
  Add CLI switches only after the underlying behavior exists.
- Acceptance: deterministic tests demonstrate each implemented switch changes
  a relevant decision; fork, synchronous wake, asymmetric capacity, and debt
  cases follow C6. Keep unrelated placement fallback behavior unchanged.
- Depends on: A1 for kernel wake-flag integration.
- Implemented: wake history/wide/affine/fork selection, synchronous/weighted
  wake switches, projected reference/current time, debt and blocked-task
  restrictions, protection shortening and idle-cgroup preemption. Targeted
  tests pass. Reference projection and hrtick share the uncharged-service
  calculation to avoid subtracting accounted runtime twice.
- Remaining: full recent-used/cache/asymmetric/SMT placement and corresponding
  event-driven latency and feature-toggle scenarios.

### A3. Cgroup shares, idle and bandwidth — in progress

- Scope: `eevdf/cgroup.go`, `eevdf/bandwidth.go`, shared quota state, cgroupfs
  readers and BPF cgroup callbacks.
- Implement C11 group load/share aggregation and cpu.idle semantics; propagate
  configuration consistently to all owners.
- Read/enforce cpu.max.burst, including all ancestors and shared quota pools.
- Replace rejection of dynamic sharded quota changes only when configuration
  publication and already-reserved slices have a verified ordering protocol.
- Acceptance: hierarchical weight ratios, idle groups, cross-CPU shares,
  burst bounds and dynamic multi-owner updates have explicit tests.
- Depends on: A1 for optional kernel callbacks; shared configuration protocol
  precedes enabling dynamic sharded quota changes.
- Implemented: actual-root nice-weight preservation, cpu.idle propagation and
  owner-local cross-CPU shares. Root/idle configuration and wake state survive
  the shard integration. Four-CPU simulation verifies 200:100 groups (four
  tasks each) within 5%, and four-task 50ms/100ms quota within one slice.
- Remaining: PELT/cpuset-aware and cross-owner aggregates, burst, dynamic
  sharded quota updates, and privileged cgroup acceptance.

### A4. Runtime recovery and lifecycle — in progress

- Scope: BPF heartbeat, userscheduler wakeup, single/sharded loops and task GC.
- Add the planned periodic kernel heartbeat, bounded dead-task collection and
  explicit kernel-exit/error handling; retain prompt signal handling.
- Pin/lock memory and tune GC with explicit errors; avoid introducing an
  unverified yield/wakeup protocol.
- Acceptance: dropped exit/sleep recovery tests, bounded incremental GC,
  heartbeat callback tests, and VM progress under idle/busy workloads.
- Depends on: A1; BPF progress validation in A7 before claiming verified.
- Implemented: 1ms kernel heartbeat targets, registered scheduler TIDs,
  explicit kernel-exit checks, memlock/GC setup and incremental dead-PID GC
  (64 entries / 200us budget per call, with continuation across calls).
  Unit/callback tests pass; current runtime can attach in the disposable VM.
- Remaining: full dropped-event/churn simulation, PID-reuse recovery analysis,
  and a verified yield/wakeup protocol. The runtime still uses idle polling.

### A5. Hot path and data structures — in progress

- Scope: eligible tree allocation, bridge calls, runtime allocations.
- Reuse detached tree nodes; document treap expected (not worst-case) complexity
  unless replaced by a deterministic balanced tree.
- Benchmark before replacing normal cgo with runtime-internal asmcgocall.
  Preserve ABI, runtime safety and error propagation; do not copy unsafe
  assembly without layout assertions and architecture coverage.
- Acceptance: allocation benchmarks, identical tree oracle results, Go race
  tests, and architecture-specific ABI tests before enabling a new bridge.
- Depends on: stable A1/A2 interfaces.
- Implemented: detached treap node reuse. Tree oracle and allocation tests
  pass; `BenchmarkTreeReinsert` measured 20.88ns/op, 0 B/op, 0 allocs/op on the
  current host. This is a node-reinsertion microbenchmark, not whole-loop cost.
- Remaining: bridge/loop benchmarks and architecture-safe ABI optimization.
  Normal cgo is retained; no asmcgocall parity claim.

### A6. CLI, statistics and documentation — in progress

- Scope: CLI configuration, JSON statistics and monitor transport, READMEs.
- Expose effective feature and numeric/topology settings from C14; reject
  unsupported settings rather than presenting inert switches.
- Add immutable JSON snapshots, loop latency/GC/event/drop/dispatch counters,
  and then a separately readable mmap stats map for monitor mode.
- Correct claims of complete parity, red-black trees, race-test invocation and
  minimum kernel compatibility. Keep English/Chinese documentation aligned.
- Acceptance: CLI parsing tests, JSON shape tests, monitor reader/writer ABI
  tests, and actual command examples that match the implementation.
- Depends on: underlying features in A2/A3; monitor depends on snapshot schema.
- Implemented: effective `-no-vref-update`, `-no-wake-sync`, `-no-wa-weight`;
  corrected bilingual compatibility/completeness/transport documentation and
  re-rendered Manim PNG/GIF from the corrected source.
- Added opt-in `-trace-file` JSONL diagnostics for events, successful commands
  and task publications in both runtime modes. Concurrent schema and I/O-error
  tests pass. This adds overhead and is not the planned immutable stats/monitor.
- Remaining: numeric/topology options, CLI parsing tests, immutable JSON
  statistics, loop/GC latency and mmap monitor transport.

### A7. Deterministic simulation and BPF progress tests — in progress

- Scope: Go simulation/test helpers and `scripts/` BPF callback harness.
- Port T1's reference vectors without substituting different expected values.
- Add event-driven CPU/slice/hrtick/pressure/cgroup simulation and assert the
  numeric fairness, balance, latency and quota thresholds from T2.
- Add compiled mock-helper callback tests for T3, including congestion, stale
  commands, affinity changes, timer generation and seqcount fallbacks.
- Acceptance: runnable unprivileged commands; seeded reproducibility; tests
  fail for deliberately broken ordering, fairness or quota behavior.
- Depends on: each corresponding implementation; can grow incrementally.
- Implemented: deterministic slice-driven `eevdf/sim` with CPU current state,
  dispatch queues, expiry/preemption, run-sequence timers and periodic policy
  callbacks. Tests assert nice 0 vs -5 ratio 3121/1024 within 5%, four-CPU
  cgroup/quota thresholds, service conservation and repeatability.
  The matching VM nice 0/6 case now also runs on an eight-CPU topology with
  both tasks restricted to CPU0, using the observed sched_ext weight 27 and
  requiring the standard 272/1024 service ratio within 5%.
- Callback harness compiles actual source bodies for enqueue fields and
  backlog-before-publication, congestion fallback, dispatch guards,
  continuation, hrtick, heartbeat, seqcount/PID/FIFO fallback and displacement.
  Continuation also checks the kernel local DSQ after draining commands,
  preventing an empty Go tree from authorizing a refill over locally queued work.
  It passes unprivileged with `python3 scripts/test_bpf_progress.py`.
- Remaining: exact T1 vector audit; simulator sleep/wakeup/release/drop
  injection and all remaining T2 topology/latency/feature scenarios.
  The slice model is not a full kernel callback-ordering emulator.

### A8. VM acceptance and performance — in progress; smoke/fairness slice verified

- Scope: `scripts/` disposable-VM test entry points and recorded results.
- Run T4 smoke/fairness/yield/churn/cgroup/pressure/hrtick/core-scheduling and
  long-run workloads on a compatible kernel. Never attach the experimental
  scheduler to a shared host merely to satisfy acceptance.
- Compare against built-in EEVDF and test both sharding modes; capture kernel
  version, configuration, workload parameters and measured thresholds.
- Acceptance: actual T4 numbers, no watchdog failures, and explicit skips for
  unavailable kernel features. Compilation/unit tests are not substitutes.
- Depends on: A1–A7 as applicable and an available disposable VM.
- Implemented: marked-guest-only smoke entry points using a read-only host
  root, privileged load tests, bounded attachment readiness, weighted CPU-time
  sampling, signal cleanup and normal-path detach verification.
- Historical failure on `6.14.0-37-generic`, 8 CPUs / 4GiB:
  an earlier smoke recorded nice 0/6 ratio 2.667 and successful detach,
  but a later default run measured 2.047
  (`[307,150]` ticks), below the required [2.5,5.0]. Disabling VrefUpdate also
  failed at 2.403 (`[334,139]`), so projection alone does not explain the gap.
  After the local-DSQ continuation guard, load/attach still pass but fairness
  remains below threshold at 2.317 (`[336,145]`).
- Root cause identified on 2026-10-29: the benchmark sampled one second after
  process creation, not after worker initialization. Runtime traces showed the
  intended nice-6 worker still reporting weight 100 through startup/import
  events, then switching to 27 only after `os.nice(6)`. Thus the measurement
  included work before the intended priority and CPU-bound loop were active.
  This was a benchmark synchronization error, not evidence that the weighted
  policy itself required a fairness correction.
- Fix: both workers acknowledge PID, actual nice and affinity after setup;
  warmup and sampling begin only after both acknowledgements. Missing, invalid
  or timed-out readiness fails explicitly. Six unprivileged regression tests
  cover delayed startup, real nice/affinity, invalid readiness, EOF, timeout
  and no sampling on a failed barrier. The original [2.5,5.0] bound is unchanged.
- Verification, same kernel / 8 CPUs / 4GiB / six-second measurement, tracing
  disabled: built-in EEVDF 3.762 (`[474,126]` ticks); three consecutive
  attach/fairness/detach runs 3.690 (`[417,113]`), 3.754 (`[428,114]`),
  3.711 (`[423,114]`). A separate post-fix run measured 3.701 (`[433,117]`).
  The updated documented VM entry point (including the built-in baseline)
  also passes at 3.737 (`[426,114]`) with successful signal detach.
  Runtime tracing with readiness also measured 410/110 = 3.727, but diagnostic
  tracing perturbs timing and is not used as performance acceptance.
- The VM smoke now records kernel/CPU metadata and tests the built-in baseline
  before attaching the experimental scheduler.
- Adaptive wake-latency gate remains **failing**, independently of CPU fairness.
  `schtest` revision `ae6450add75784793c691207ae1bc1df1bf9edee`
  measures p90 fast/slow semaphore wake latency on one CPU, with an unchanged
  `<0.50` threshold. The actual fast workload spins for 100 us and sleeps
  9.9 ms; the slow workload spins for 10 ms.
  The local benchmark used cgroup-v1 `tasks`; a session-local source copy
  changed only cgroup membership/cleanup accesses to `cgroup.procs` for the
  cgroup-v2 VM. Workload, statistics and threshold were not modified.
- Corrected two wake-path problems: both runtime loops now wait on ring epoll
  readiness instead of an uninterruptible 100 us sleep, with a 1 ms timeout
  for periodic work/mailboxes/shutdown; normal BPF enqueue no longer issues
  an unconditional PREEMPT kick before Go's wake-preemption decision.
  Privileged tests cover empty timeout, already-ready repeated waits,
  non-consuming waits and delayed delivery. The actual-source callback
  harness asserts enqueue publication does not preempt.
- Reproducible entry point: `scripts/vm_adaptive.sh /path/to/schtest` inside
  a marked disposable VM. It runs built-in EEVDF and three independent
  attach/test/detach attempts, propagating any benchmark failure.
  Final untraced run, `6.14.0-37-generic`, 8 CPUs / 4GiB:
  built-in EEVDF **0.78**; scheduler **0.88 / 0.84 / 0.77**. All failed.
  Earlier scheduler measurements before the wake-path changes included 1.33;
  VM variability and a failing built-in baseline prevent attributing an
  end-to-end improvement to these changes alone.
  The post-change CPU-fairness smoke still passes: built-in 3.762
  (`[474,126]` ticks), scheduler 3.733 (`[448,120]`), normal detach.
- Next adaptive-priority work: correlate actual sched_switch timestamps with
  wake events and command execution, separate transport delay from policy
  deferral, and repeat the unchanged gate on a quiet VM with a stable built-in
  baseline. A diagnostic trace already shows substantial enqueue-to-command
  delay, but tracing itself perturbs latency; it is not acceptance evidence.
  Do not add arbitrary sleeper bonuses or relax the benchmark to hide the gap.
- Remaining: every other T4 row, broader built-in EEVDF performance comparison,
  64+CPU sharded workloads and long-run/performance measurements.
  Smoke/fairness passing on this kernel does not verify all of A8.

## Current validation baseline

- `make build`, `go test ./...`, `go test -race ./...`, `go vet ./...`,
  `python3 scripts/test_bpf_progress.py`, and `git diff --check` pass.
- Privileged BPF load/optional-op/shard-load tests pass in the disposable VM.
  Worker-readiness regression tests and the corrected VM weighted-fairness
  smoke pass as documented under A8; the full T4 matrix remains incomplete.
- `sh -n scripts/vm_adaptive.sh` and its shared-host rejection guard pass.
  Its actual VM baseline and three scheduler runs correctly return failure;
  adaptive wake-latency acceptance is explicitly unresolved.
- Keep A1–A8 distinct from the original plan's similarly named task IDs.
  READMEs and Manim diagrams describe architecture, not proof of full parity.
