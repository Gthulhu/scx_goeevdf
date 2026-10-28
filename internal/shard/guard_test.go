package shard

import (
	"testing"

	"github.com/Gthulhu/scx_goeevdf/eevdf"
)

type fakeIO struct{ sent int }

func (i *fakeIO) Submit(eevdf.Cmd) error          { i.sent++; return nil }
func (i *fakeIO) Idle(int32) bool                 { return true }
func (i *fakeIO) CurrPid(int32) int32             { return 0 }
func (i *fakeIO) SetQueued(int32, uint64, uint64) {}

func TestGuardRejectsForeignCPUCommandsAndQueueWrites(t *testing.T) {
	topo, err := NewTopology(80, [][]int32{
		func() []int32 {
			a := make([]int32, 40)
			for j := range a {
				a[j] = int32(j)
			}
			return a
		}(),
		func() []int32 {
			a := make([]int32, 40)
			for j := range a {
				a[j] = int32(j + 40)
			}
			return a
		}(),
	}, false)
	if err != nil {
		t.Fatal(err)
	}
	io := &fakeIO{}
	g := GuardIO{Shard: 0, Topology: topo, IO: io}
	if err := g.Submit(eevdf.Cmd{Kind: eevdf.CmdDispatch, Cpu: 40}); err == nil {
		t.Fatal("foreign dispatch accepted")
	}
	if err := g.Submit(eevdf.Cmd{Kind: eevdf.CmdKick, Cpu: 0}); err != nil || io.sent != 1 {
		t.Fatal("local command rejected")
	}
	defer func() {
		if recover() == nil {
			t.Error("foreign queue write did not panic")
		}
	}()
	g.SetQueued(40, 1, 0)
}
