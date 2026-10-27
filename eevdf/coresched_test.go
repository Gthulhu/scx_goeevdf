package eevdf

import "testing"

type publicationIO struct {
	fakeIO
	pubs []struct {
		slot uint32
		pub  TaskPub
	}
	now uint64
}

func (i *publicationIO) Publish(slot uint32, pub TaskPub) {
	i.pubs = append(i.pubs, struct {
		slot uint32
		pub  TaskPub
	}{slot, pub})
}
func (i *publicationIO) Now() uint64 { return i.now }

func TestCorePublisherSlotReuseClearsOldPID(t *testing.T) {
	io := &publicationIO{fakeIO: fakeIO{n: make([]uint64, 1)}}
	p := New(io, 1, Enums{})
	s := NewCorePublisher(3)
	a, b, overflow := &Task{Pid: 123}, &Task{Pid: 456}, &Task{Pid: 789}
	if s.Allocate(a) != 1 || s.Allocate(a) != 1 || s.Allocate(b) != 2 || s.Allocate(overflow) != 0 {
		t.Fatal("allocated duplicate or reserved/overflow slot", a.Slot, b.Slot, overflow.Slot)
	}
	a.Weight, a.Vruntime, a.Req.Deadline = Nice0Weight, 90, 110
	p.packs[0].join(a)
	s.PublishTask(p, a, 17)
	if len(io.pubs) != 1 || io.pubs[0].pub.Pid != 123 || io.pubs[0].pub.Weight != Nice0Weight {
		t.Fatal("missing publication", io.pubs)
	}
	s.Release(p, a)
	if len(io.pubs) != 2 || io.pubs[1].slot != 1 || io.pubs[1].pub != (TaskPub{}) || a.Slot != 0 {
		t.Fatal("slot was not zeroed before reuse", io.pubs)
	}
	if s.Allocate(overflow) != 1 {
		t.Fatal("freed slot not reused")
	}
	s.PublishTask(p, overflow, 18)
	if len(io.pubs) != 3 || io.pubs[2].pub.Pid != 789 {
		t.Fatal("new PID publication missing", io.pubs)
	}
}

func TestCoreVtimeReanchorsAfterPackEmpties(t *testing.T) {
	io := &publicationIO{fakeIO: fakeIO{n: make([]uint64, 1)}}
	p := New(io, 1, Enums{})
	a := &Task{Pid: 1, Weight: Nice0Weight, Vruntime: 100}
	pk := p.packs[0]
	pk.join(a)
	a.Vruntime = 120
	if v, ok := p.coreVtime(a, 0); !ok || v != 20 {
		t.Fatalf("first epoch: %d %v", v, ok)
	}
	pk.leave(a, true)
	if _, ok := p.coreVtime(a, 0); ok {
		t.Fatal("unpacked task still has core coordinate")
	}
	b := &Task{Pid: 2, Weight: Nice0Weight, Vruntime: 50}
	pk.join(b)
	if v, ok := p.coreVtime(b, 0); !ok || v != 0 {
		t.Fatalf("new epoch retained old core origin: %d %v", v, ok)
	}
}
