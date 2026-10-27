package shard

import (
	"github.com/Gthulhu/scx_goeevdf/eevdf"
)

// PolicyEngine is a real per-shard Policy adapter. Its underlying Policy has
// only local CPUs; task images are exported from source packs and adopted into
// destination packs. The caller must pass a different command-ring IO for
// each shard. Concurrent access must go through Coordinator.Step/Run.
type PolicyEngine struct {
	Shard      *eevdf.ShardPolicy
	quotaBound bool
}

func NewPolicyEngine(io eevdf.IO, cpus []int32, enums eevdf.Enums) (*PolicyEngine, error) {
	p, err := eevdf.NewShardPolicy(io, cpus, enums)
	if err != nil {
		return nil, err
	}
	return &PolicyEngine{Shard: p}, nil
}

// NewPolicyEngineWithQuota binds a shared pool before the Policy can be owned
// by Run. Use one pool for every PolicyEngine in a multi-shard coordinator.
func NewPolicyEngineWithQuota(io eevdf.IO, cpus []int32, enums eevdf.Enums, pool *eevdf.SharedQuotaPool) (*PolicyEngine, error) {
	e, err := NewPolicyEngine(io, cpus, enums)
	if err != nil {
		return nil, err
	}
	if err := pool.Bind(e.Shard.Policy); err != nil {
		return nil, err
	}
	e.quotaBound = true
	return e, nil
}

func (e *PolicyEngine) Handle(slot int32, event eevdf.Event) error {
	return e.Shard.Handle(slot, event)
}

func (e *PolicyEngine) HandleForTransfer(slot int32, event eevdf.Event, sourceCPU int32) error {
	return e.Shard.HandleForTransfer(slot, event, sourceCPU)
}

func (e *PolicyEngine) CanTransfer(pid, slot int32) bool {
	return e.Shard.CanTransfer(pid, slot)
}

func (e *PolicyEngine) AffinityTarget(pid int32) (int32, bool, error) {
	return e.Shard.AffinityTarget(pid)
}

func (e *PolicyEngine) Export(pid, slot int32) (Image, error) {
	return e.Shard.Export(pid, slot)
}

func (e *PolicyEngine) Import(pid, slot, cpu int32, image Image) error {
	return e.Shard.Import(pid, slot, cpu, image)
}

func (e *PolicyEngine) Steal(from, dst int32, level int) (int32, int32, bool, error) {
	return e.Shard.Steal(from, dst, level)
}

func (e *PolicyEngine) StopFor(cpu, pid, dst int32) error {
	return ErrActiveStopUnsupported
}

// SupportsStopFor remains false until the BPF transport can acknowledge that
// the task is no longer running and the source Policy has queued it for export.
func (e *PolicyEngine) SupportsStopFor() bool { return false }

func (e *PolicyEngine) Tick() error {
	e.Shard.Tick()
	return nil
}

func (e *PolicyEngine) Periodic(now uint64) {
	e.Shard.Policy.Periodic(now)
	e.Shard.Policy.BalanceTick(now)
}

// Configure runs only inside this engine's owner Step/Run callback.
func (e *PolicyEngine) Configure(c Config) error {
	p := e.Shard.Policy
	switch c.Kind {
	case ConfigGroup:
		return p.SetCgroup(c.ID, c.Parent, c.Weight)
	case ConfigQuota:
		return p.SetCPUQuota(c.ID, c.Quota, c.Period)
	case ConfigRemoveGroup:
		p.RemoveCgroup(c.ID)
		return nil
	default:
		return c.validate()
	}
}

func (e *PolicyEngine) SharedQuotaBound() bool { return e.quotaBound }

func (e *PolicyEngine) Snapshot(cpu int32) Values {
	v := e.Shard.Snapshot(cpu)
	return Values{
		Load: v.Load, Util: v.Util, W: v.W, Queued: v.Queued,
		BalanceCap: v.BalanceCap, WakeLoad: v.WakeLoad,
	}
}
