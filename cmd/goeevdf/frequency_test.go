package main

import (
	"testing"

	"github.com/Gthulhu/scx_goeevdf/eevdf"
)

type frequencyIO struct{ perf []uint64 }

func (*frequencyIO) Submit(eevdf.Cmd) error          { return nil }
func (*frequencyIO) Idle(int32) bool                 { return true }
func (*frequencyIO) CurrPid(int32) int32             { return 0 }
func (*frequencyIO) SetQueued(int32, uint64, uint64) {}
func (i *frequencyIO) SetPerf(c int32, v uint64)     { i.perf[c] = v }

func TestFrequencyHeartbeatAndAttach(t *testing.T) {
	io := &frequencyIO{perf: make([]uint64, 1)}
	p := eevdf.New(io, 1, eevdf.Enums{})
	g := eevdf.NewFrequencyGovernor(1, 10_000_000)
	updateFrequency(p, g, io, 1_000_000_000, 1, nil)
	if io.perf[0] != 1 {
		t.Fatalf("idle perf target = %d, want minimum", io.perf[0])
	}
	p.Handle(eevdf.Event{Kind: eevdf.EvEnqueue, Pid: 42, Cpu: 0,
		Weight: 100, EnqSeq: 1, Now: 1_001_000_000})
	updateFrequency(p, g, io, 1_001_000_000, 1, map[int32]bool{0: true})
	if io.perf[0] != 1024 {
		t.Fatalf("attach perf target = %d, want full speed", io.perf[0])
	}
	p.F.Cpufreq = false
	updateFrequency(p, g, io, 1_100_000_000, 1, nil)
	if io.perf[0] != 1024 {
		t.Fatal("disabled governor unexpectedly published")
	}
}
