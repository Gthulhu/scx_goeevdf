package bpf

import (
	"os"
	"path/filepath"
	"sync/atomic"
	"syscall"
	"testing"
	"time"
	"unsafe"
)

func TestWireLayout(t *testing.T) {
	tests := []struct {
		name      string
		got, want uintptr
	}{
		{"event", unsafe.Sizeof(Ev{}), 128},
		{"command", unsafe.Sizeof(Cmd{}), 64},
		{"CPU", unsafe.Sizeof(CpuSlot{}), 64},
		{"control", unsafe.Sizeof(CpuCtl{}), 64},
		{"task", unsafe.Sizeof(TaskPub{}), 64},
	}
	for _, tt := range tests {
		if tt.got != tt.want {
			t.Errorf("%s size = %d, want %d", tt.name, tt.got, tt.want)
		}
	}
}

func TestSharedControl(t *testing.T) {
	s := &Sched{CPU: make([]CpuSlot, 2), Ctl: make([]CpuCtl, 2), Task: make([]TaskPub, 2)}
	s.Publish(1, TaskPub{Pid: 123, CoreVtime: 20, Vruntime: 30, Deadline: 40, Lag: -5, Weight: 100})
	if pub := s.Task[1]; pub.Seq != 2 || pub.Pid != 123 || pub.CoreVtime != 20 ||
		pub.Vruntime != 30 || pub.Deadline != 40 || pub.Lag != -5 || pub.Weight != 100 {
		t.Fatalf("published task: %+v", pub)
	}
	if s.Task[1].PublishedAt == 0 || s.Task[1].PublishedAt == ^uint64(0) {
		t.Fatal("publication timestamp not recorded")
	}
	s.Publish(1, TaskPub{Pid: 124})
	if s.Task[1].Seq != 4 || s.Task[1].Pid != 124 {
		t.Fatalf("sequence not advanced: %+v", s.Task[1])
	}
	s.Publish(1, TaskPub{Pid: 124, PublishedAt: 17})
	if s.Task[1].Seq != 6 || s.Task[1].PublishedAt <= 17 {
		t.Fatalf("publication clock not recorded: %+v", s.Task[1])
	}
	s.Publish(1, TaskPub{})
	if s.Task[1].Seq != 8 || s.Task[1].PublishedAt != 0 {
		t.Fatalf("released publication not cleared: %+v", s.Task[1])
	}
	s.Publish(0, TaskPub{Pid: 125})
	s.Publish(2, TaskPub{Pid: 125})
	if s.Task[0].Seq != 0 {
		t.Fatal("reserved slot was modified")
	}
	atomic.StoreUint64(&s.CPU[1].RunSeq, 42)
	if s.RunSeq(1) != 42 || s.RunSeq(-1) != 0 || s.RunSeq(2) != 0 {
		t.Fatal("invalid run sequence read")
	}
	s.SetPerf(1, 2000)
	if s.Ctl[1].PerfTarget != 1024 {
		t.Fatalf("uncapped performance target: %d", s.Ctl[1].PerfTarget)
	}
	s.SetPerf(1, 0)
	s.SetPerf(-1, 100)
	s.SetPerf(2, 100)
	if s.Ctl[1].PerfTarget != 0 || s.Ctl[0].PerfTarget != 0 {
		t.Fatal("invalid performance update")
	}
}

func TestOpenAndSetTgid(t *testing.T) {
	path := filepath.Join("..", "..", "bpf", "goeevdf.bpf.o")
	if _, err := os.Stat(path); err != nil {
		t.Skip("run make bpf first")
	}
	s, err := OpenPath(path)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	if err := s.WaitShard(0); err != syscall.EINVAL {
		t.Fatalf("unloaded ring wait: %v", err)
	}
	if err := s.SetUserschedTgid(uint32(os.Getpid())); err != nil {
		t.Fatal(err)
	}
	if err := s.SetShards([]uint32{0, 1, 1, 0}, 2); err != nil {
		t.Fatal(err)
	}
	if s.ShardForCPU(1) != 1 || s.ShardForCPU(3) != 0 || s.ShardForCPU(100) != 0 {
		t.Fatal("incorrect CPU ownership")
	}
	if err := s.SetShards([]uint32{2}, 2); err != syscall.EINVAL {
		t.Fatalf("out-of-range shard accepted: %v", err)
	}
	if err := s.SetShards(nil, 0); err != syscall.EINVAL {
		t.Fatalf("zero shards accepted: %v", err)
	}
	if err := s.SubmitShard(0, &Cmd{Kind: CmdDispatch, Cpu: 1}); err != syscall.EINVAL {
		t.Fatalf("off-shard dispatch accepted: %v", err)
	}
	if _, err := s.ConsumeShard(2, &Ev{}); err != syscall.EINVAL {
		t.Fatalf("invalid consumer accepted: %v", err)
	}
	if got := s.mapTable("missing"); got != -int(syscall.ENOENT) {
		t.Fatalf("missing map fd = %d", got)
	}
}

func TestWaitShardInvalid(t *testing.T) {
	s := &Sched{nrShards: 1}
	for _, id := range []int{-1, 0, 1} {
		if err := s.WaitShard(id); err != syscall.EINVAL {
			t.Fatalf("wait on unavailable shard %d: %v", id, err)
		}
	}
}

func TestPrivilegedLoad(t *testing.T) {
	if os.Getenv("GOEEVDF_TEST_BPF_LOAD") == "" {
		t.Skip("set GOEEVDF_TEST_BPF_LOAD=1 to load kernel programs")
	}

	s, err := OpenPath(filepath.Join("..", "..", "bpf", "goeevdf.bpf.o"))
	if err != nil {
		t.Fatal(err)
	}

	defer s.Close()
	if err := s.SetUserschedTgid(uint32(os.Getpid())); err != nil {
		t.Fatal(err)
	}
	if err := s.Load(); err != nil {
		t.Fatal(err)
	}
	if s.CPU == nil || s.Ctl == nil || s.Task == nil {
		t.Fatal("shared tables not mapped")
	}
	if err := s.Submit(&Cmd{Kind: CmdKick, Cpu: 0}); err != nil {
		t.Fatalf("submit to unattached command ring: %v", err)
	}
	if err := s.Kick(0, 0); err != nil {
		t.Fatalf("invoke kick syscall program: %v", err)
	}
	if err := s.Submit(&Cmd{Kind: CmdHrtick, Cpu: 0, Slice: 1, Slot: 1}); err != nil {
		t.Fatalf("invoke hrtick syscall program: %v", err)
	}
	var ev Ev
	if ok, err := s.Consume(&ev); ok || err != nil {
		t.Fatalf("unexpected event: ok=%v err=%v", ok, err)
	}
	start := time.Now()
	if err := s.WaitShard(0); err != nil {
		t.Fatal(err)
	}
	if elapsed := time.Since(start); elapsed > time.Second {
		t.Fatalf("empty ring wait blocked for %s", elapsed)
	}
	s.SetNrScheduled(42)
	if got := s.Counter("nr_scheduled"); got != 42 {
		t.Fatalf("scheduled count = %d, want 42", got)
	}
	const events = 8
	for seq := uint64(1); seq <= events; seq++ {
		if err := s.testEmit(0, seq); err != nil {
			t.Fatalf("emit event %d: %v", seq, err)
		}
	}
	if got := s.Counter("nr_queued"); got != events {
		t.Fatalf("queued count before consume = %d, want %d", got, events)
	}
	start = time.Now()
	for i := 0; i < 100; i++ {
		if err := s.WaitShard(0); err != nil {
			t.Fatalf("wait on already queued events: %v", err)
		}
	}
	if elapsed := time.Since(start); elapsed > 50*time.Millisecond {
		t.Fatalf("ready ring repeatedly timed out instead of waking: %s", elapsed)
	}
	for seq := uint64(1); seq <= events; seq++ {
		var got Ev
		ok, err := s.Consume(&got)
		if err != nil || !ok || got.Kind != EvEnqueue || got.EnqSeq != seq {
			t.Fatalf("consume #%d: ok=%v err=%v event=%+v", seq, ok, err, got)
		}
		s.DecNrQueued(1)
	}
	if got := s.Counter("nr_queued"); got != 0 {
		t.Fatalf("queued count after consume = %d, want 0", got)
	}
	if ok, err := s.Consume(&ev); ok || err != nil {
		t.Fatalf("extra event: ok=%v err=%v", ok, err)
	}
	emitted := make(chan error, 1)
	go func() {
		time.Sleep(5 * time.Millisecond)
		emitted <- s.testEmit(0, 99)
	}()
	deadline := time.Now().Add(time.Second)
	for {
		if err := s.WaitShard(0); err != nil {
			t.Fatal(err)
		}
		ok, err := s.Consume(&ev)
		if err != nil {
			t.Fatal(err)
		}
		if ok {
			if ev.EnqSeq != 99 {
				t.Fatalf("wait consumed or reordered event: %+v", ev)
			}
			s.DecNrQueued(1)
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("delayed event did not wake consumer")
		}
	}
	if err := <-emitted; err != nil {
		t.Fatal(err)
	}
}

func TestPrivilegedLoadOptionalOps(t *testing.T) {
	if os.Getenv("GOEEVDF_TEST_BPF_LOAD") == "" {
		t.Skip("set GOEEVDF_TEST_BPF_LOAD=1 to load kernel programs")
	}

	for _, name := range []string{"ge_tick", "ge_core_sched_before", "ge_dump_task"} {
		t.Run(name, func(t *testing.T) {
			s, err := OpenPath(filepath.Join("..", "..", "bpf", "goeevdf.bpf.o"))
			if err != nil {
				t.Fatal(err)
			}
			defer s.Close()
			if err := s.SetUserschedTgid(uint32(os.Getpid())); err != nil {
				t.Fatal(err)
			}
			if err := s.DisableOp(name); err != nil {
				t.Fatal(err)
			}
			if err := s.Load(); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestPrivilegedShardLoad(t *testing.T) {
	if os.Getenv("GOEEVDF_TEST_BPF_LOAD") == "" {
		t.Skip("set GOEEVDF_TEST_BPF_LOAD=1 to load kernel programs")
	}
	s, err := OpenPath(filepath.Join("..", "..", "bpf", "goeevdf.bpf.o"))
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	if err := s.SetUserschedTgid(uint32(os.Getpid())); err != nil {
		t.Fatal(err)
	}
	if err := s.SetShards([]uint32{0, 1}, 2); err != nil {
		t.Fatal(err)
	}
	if err := s.Load(); err != nil {
		t.Fatal(err)
	}
	if err := s.RegisterUsersched(uint32(syscall.Gettid()), 1); err != nil {
		t.Fatal(err)
	}
	if err := s.SubmitShard(1, &Cmd{Kind: CmdKick, Cpu: 1}); err != nil {
		t.Fatal(err)
	}
	if err := s.SubmitShard(0, &Cmd{Kind: CmdKick, Cpu: 1}); err != syscall.EINVAL {
		t.Fatalf("cross-shard command: %v", err)
	}
	if err := s.testEmit(1, 21); err != nil {
		t.Fatal(err)
	}
	var ev Ev
	if ok, err := s.ConsumeShard(0, &ev); ok || err != nil {
		t.Fatalf("unexpected shard 0 event: %v %v", ok, err)
	}
	if ok, err := s.ConsumeShard(1, &ev); !ok || err != nil || ev.EnqSeq != 21 {
		t.Fatalf("wrong shard event: %v %v %+v", ok, err, ev)
	}
	s.DecNrQueuedShard(1, 1)
	if got := s.NrQueuedShard(1); got != 0 {
		t.Fatalf("shard count = %d", got)
	}
}
