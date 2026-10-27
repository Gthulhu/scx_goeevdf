package shard

import (
	"fmt"

	"github.com/Gthulhu/scx_goeevdf/eevdf"
)

// GuardIO enforces the command ownership invariant at the Policy IO boundary.
// Construct a separate guard for each shard and give it only that shard's IO
// ring. Remote CPU reads are allowed, but writes and scheduling commands are
// rejected. Never share a Policy between shard engines.
type GuardIO struct {
	Shard    int
	Topology Topology
	IO       eevdf.IO
}

func (g GuardIO) owns(cpu int32) bool {
	id, ok := g.Topology.OwnerCPU(cpu)
	return ok && id == g.Shard
}

func (g GuardIO) Submit(cmd eevdf.Cmd) error {
	if !g.owns(cmd.Cpu) {
		return fmt.Errorf("shard %d cannot submit command for CPU %d", g.Shard, cmd.Cpu)
	}
	return g.IO.Submit(cmd)
}

func (g GuardIO) Idle(cpu int32) bool     { return g.IO.Idle(cpu) }
func (g GuardIO) CurrPid(cpu int32) int32 { return g.IO.CurrPid(cpu) }

func (g GuardIO) SetQueued(cpu int32, n, keepSlice uint64) {
	if !g.owns(cpu) {
		panic(fmt.Sprintf("shard %d cannot update queue for CPU %d", g.Shard, cpu))
	}
	g.IO.SetQueued(cpu, n, keepSlice)
}
