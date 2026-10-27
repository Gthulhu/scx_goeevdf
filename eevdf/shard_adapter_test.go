package eevdf

import (
	"bytes"
	"encoding/gob"
	"errors"
	"testing"
)

type shardTestIO struct{}

func (shardTestIO) Submit(Cmd) error                { return errors.New("keep queued for migration") }
func (shardTestIO) Idle(int32) bool                 { return false }
func (shardTestIO) CurrPid(int32) int32             { return 0 }
func (shardTestIO) SetQueued(int32, uint64, uint64) {}
func (shardTestIO) Affinity(int32) ([]int32, error) { return []int32{4, 8}, nil }

func TestShardPendingImageAndLag(t *testing.T) {
	src, err := NewShardPolicy(shardTestIO{}, []int32{4}, Enums{})
	if err != nil {
		t.Fatal(err)
	}
	dst, err := NewShardPolicy(shardTestIO{}, []int32{8}, Enums{})
	if err != nil {
		t.Fatal(err)
	}
	if err := src.Handle(17, Event{Kind: EvEnqueue, Pid: 42, Tgid: 41,
		Cpu: 4, Weight: 100, SumExec: 123, EnqSeq: 9}); err != nil {
		t.Fatal(err)
	}
	src.Tick()
	orig := src.Policy.tasks[42]
	orig.Vruntime = 1234567
	orig.Req = Request{Deadline: 1239999, Slice: 10_000, Weight: orig.Weight}
	orig.lastSum = 123
	originalLag := src.Policy.packs[0].lag(orig, src.Policy.Slice+src.Policy.TickNs)
	image, err := src.Export(42, 17)
	if err != nil {
		t.Fatal(err)
	}
	var decoded shardTaskImage
	if err := gob.NewDecoder(bytes.NewReader(image)).Decode(&decoded); err != nil || decoded.Lag != originalLag {
		t.Fatalf("lost source lag in transit: %d != %d, error %v", decoded.Lag, originalLag, err)
	}
	if src.Policy.tasks[42] != nil || src.Policy.trees[0].Len() != 0 ||
		src.Policy.packs[0].W != 0 {
		t.Fatal("export left task in source Policy")
	}
	if err := dst.Import(42, 17, 8, image); err != nil {
		t.Fatal(err)
	}
	imported := dst.Policy.tasks[42]
	if imported == nil || imported.Tgid != 41 || imported.EnqSeq != 9 ||
		imported.Slot != 17 || imported.lastSum != 123 ||
		dst.Policy.packs[0].W != Nice0Weight || dst.Policy.trees[0].Len() != 1 {
		t.Fatalf("import did not reconstruct task: %+v", imported)
	}
	if imported.Req.Slice != 10_000 || imported.Req.Weight != Nice0Weight {
		t.Fatalf("request lost in transit: %+v", imported.Req)
	}
	if err := dst.Import(42, 17, 8, image); err == nil {
		t.Fatal("duplicate import succeeded")
	}
	if err := src.Import(42, 17, 4, []byte{0xff}); err == nil || src.Policy.tasks[42] != nil {
		t.Fatal("invalid image mutated source")
	}
}

func TestShardExportRejectsDispatchedTaskWithoutMutation(t *testing.T) {
	s, err := NewShardPolicy(shardTestIO{}, []int32{4}, Enums{})
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Handle(19, Event{Kind: EvEnqueue, Pid: 19, Cpu: 4, Weight: 100}); err != nil {
		t.Fatal(err)
	}
	task := s.Policy.tasks[19]
	task.state = Dispatched
	weight := s.Policy.packs[0].W
	if _, err := s.Export(19, 19); err == nil {
		t.Fatal("exported a running task")
	}
	if s.Policy.tasks[19] != task || s.Policy.packs[0].W != weight {
		t.Fatal("failed export mutated Policy")
	}
}

func TestShardExportRefusesUnsharedQuota(t *testing.T) {
	s, err := NewShardPolicy(shardTestIO{}, []int32{4}, Enums{})
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Policy.SetCgroup(7, 0, 100); err != nil {
		t.Fatal(err)
	}
	if err := s.Policy.SetCPUQuota(7, 100_000, 10_000_000); err != nil {
		t.Fatal(err)
	}
	if err := s.Handle(20, Event{Kind: EvEnqueue, Pid: 20, Cpu: 4, Cgid: 7, Weight: 100}); err != nil {
		t.Fatal(err)
	}
	task := s.Policy.tasks[20]
	if _, err := s.Export(20, 20); err == nil {
		t.Fatal("migrated quota-controlled task without shared accounting")
	}
	if s.Policy.tasks[20] != task || s.Policy.packs[0].W != task.Weight {
		t.Fatal("failed quota export modified source")
	}
}
