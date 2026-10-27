# Go per-LLC coordinator (Part E)

This package implements CPU-to-shard mapping, slot/PID ownership, event routing,
serialized per-shard policy callbacks, deterministic mailbox stepping, asynchronous
steal requests, copy-on-publish CPU snapshots, and explicit task handoff.
`NewPolicyEngine` creates one independent `eevdf.Policy` for each shard and
exports/imports queued or sleeping tasks (excluding proxy-blocked tasks),
preserving requests, lag and task utilization history. **It does not implement BPF per-shard rings or scheduler
threads. Do not enable multi-shard scheduling with the unmodified `eevdf.Policy`
or a single shared BPF command ring.** A request to transfer a still-running
task retains source ownership without terminating the owner loop. A later
remote kernel enqueue is handled by the source with redispatch suppressed,
then its pending state is exported to the destination. Active cross-shard `StopFor` currently
returns `ErrActiveStopUnsupported` synchronously, without submitting a STOP
command or moving a running task. A future implementation needs a verified
kernel stop/reenqueue event before source export. cpu.max budgets can use a
bound shared pool; group-wide load aggregates are not yet shared across shard
policies.

Integration:

1. Call `NewTopology(possibleCPUs, llcCPULists, forceOne)` using the *possible*
   BPF CPU IDs, not online CPU count. A topology of ≤64 CPUs has one shard;
   larger topologies coalesce adjacent LLCs smaller than four CPUs. Publish
   `OwnerCPU(cpu)` to the BPF `cpu_shard` table before loading the scheduler.
2. Allocate one event ring and command ring per shard, then call
   `NewPolicyEngine(io, Coordinator.CPUs(id), enums)` for each one. The engine
   maps physical CPU IDs to the dense per-Policy CPU indices and implements
   `Engine.Handle(slot,event)`, `Export`, `Import`, `Steal`, `Tick`, and
   `Snapshot`. Call `Coordinator.Periodic(id, now)` on the scheduler's
   CLOCK_MONOTONIC heartbeat: it *queues* the timestamp for that shard's
   owner loop, which runs local Periodic/BalanceTick before Schedule.
   The caller never invokes Policy methods. Pass IO implementing
   `eevdf.AffinityIO` to permit cross-shard
   migrations; unsupported/unknown affinity fails closed. A custom engine
   must follow the same ownership and immutable-image contract. `GuardIO` can
   reject foreign CPU commands at the physical command ring boundary, but
   wrapping an IO with it hides optional affinity/run-sequence interfaces;
   preserve those interfaces when wiring the real IO.
   `eevdf.New` initializes every CPU control entry it sees; do **not** create
   each shard Policy with the global possible-CPU count. `NewPolicyEngine`
   constructs `eevdf.New` with only the local CPU count, and its IO mapper
   translates `SetQueued` and commands to owned physical CPU IDs. Its tests
   verify initialization and later writes cannot clobber foreign CPU entries
   even when two engines share the same control-table mock.
   If cpu.max is enabled in multi-shard mode, create one
   `eevdf.SharedQuotaPool` and use `NewPolicyEngineWithQuota` for **every**
   engine; `BroadcastConfig(ConfigQuota)` rejects finite quotas unless all
   engines report a shared pool. Call `pool.Configure(id,quota,period)` once
   before broadcasting a changed quota, and `pool.Refill(now)` from shard 0
   only.
3. Convert each BPF task event to `eevdf.Event` and call `Route(bpfSlot, event)`.
   **The first kernel enqueue has slot zero:** the coordinator registers
   provisional ownership by PID and allocates a unique canonical publication
   slot (`Slot(pid)`); `Engine.Handle` installs that slot in the per-shard
   Policy, whose first DISPATCH command sends it back to BPF. Subsequent
   nonzero kernel slots must match the canonical slot. Distinct slot-zero
   enqueues never share ownership or a publication slot, even across shard
   Policies sharing `task_tbl`: `ShardPolicy.Handle` pre-creates tasks with
   their coordinator-assigned slot, bypassing each Policy's private
   `CorePublisher.Allocate` counter (which otherwise starts at 1 per Policy).
   Further wire slot-zero events (including sleep and re-enqueue) keep routing
   by PID until the first successful DISPATCH. Do not call `ShardPolicy.Handle`
   directly with the raw zero BPF slot: only `Coordinator.Route` can supply
   the global canonical slot safely.
   Idle events route
   by CPU without a task slot. A remote enqueue is first accounted on its
   source Policy with redispatch suppressed, then the pending task is exported
   and adopted by the destination. A cpumask change with no locally allowed
   CPU transfers queued/sleeping tasks; a running task waits for its next
   kernel enqueue. Other misrouted task events follow the slot owner.
   Call `Handoff`, `Steal`, or `StopFor` for
   cross-shard placement, balance, and active balance respectively. The
   scheduler must process cgroup events on shard 0 and send ordered
   `BroadcastConfig(Config{Kind: ConfigGroup|ConfigQuota|ConfigRemoveGroup,
   ...})` messages to **all** shard owners; no caller may mutate a Policy
   directly. Broadcast parent groups before children and broadcast
   configuration before routing the task enqueue that needs it. Config
   errors surface from the receiving shard's `Run`/`Step`, and must not be
   ignored. These cgroup events are not represented by `eevdf.Event`.
4. Run `Run(ctx,id)` once per shard (plus ring reader goroutines), or step
   `Step(id)` in ascending shard order for deterministic simulation (external
   Step is rejected while Run owns the shard). Each
   step drains at most 256 messages, calls `Engine.Tick`, and publishes
   complete per-CPU snapshots. A periodic timer should call `Wake(id)` so
   `Tick` runs even when its mailbox is empty. `Run` errors must be handled;
   do not silently continue after a failed policy callback. BPF wake/kick
   integration must wake a shard's scheduler thread when its mailbox receives
   a message. `Run` uses a Go wake channel, not a BPF kick.

An in-transit task has **no exposed owner**; incoming events wait for successful
adoption. A failed destination import queues restoration on the source. If
restoration also fails, the task remains in transit and restoration is retried
until the integration fixes the engine error. Neither engine may mutate an
in-transit task. Go tests cover deterministic mappings and handoffs, rollback,
foreign command rejection, concurrent routing with `-race`, and one million
completed transfers. Kernel-level tests and per-shard ring integration remain
separate work.
