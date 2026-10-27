package bpf

import (
	"os"
	"path/filepath"
	"syscall"
	"testing"
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
		{"task", unsafe.Sizeof(TaskPub{}), 56},
	}
	for _, tt := range tests {
		if tt.got != tt.want {
			t.Errorf("%s size = %d, want %d", tt.name, tt.got, tt.want)
		}
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
	if err := s.SetUserschedTgid(uint32(os.Getpid())); err != nil {
		t.Fatal(err)
	}
	if got := s.mapTable("missing"); got != -int(syscall.ENOENT) {
		t.Fatalf("missing map fd = %d", got)
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
	var ev Ev
	if ok, err := s.Consume(&ev); ok || err != nil {
		t.Fatalf("unexpected event: ok=%v err=%v", ok, err)
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
}
