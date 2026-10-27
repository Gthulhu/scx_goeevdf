package eevdf

const MaxTaskSlots uint32 = 65536

// TaskPub mirrors the data fields of the BPF task_pub; the IO implementation
// owns the seqcount around the mmap write, not the policy goroutine.
type TaskPub struct {
	Pid                           int32
	CoreVtime, Vruntime, Deadline uint64
	Lag                           int64
	Weight                        uint64
}

type publishIO interface {
	Publish(slot uint32, pub TaskPub)
}

// CorePublisher owns the slots, including the zero publication required
// before a slot can be reused by a different PID. Slot zero is reserved.
type CorePublisher struct {
	next, limit uint32
	free        []uint32
}

func NewCorePublisher(limit uint32) *CorePublisher {
	if limit > MaxTaskSlots {
		limit = MaxTaskSlots
	}
	return &CorePublisher{next: 1, limit: limit}
}

func (s *CorePublisher) Allocate(t *Task) uint32 {
	if t.Slot != 0 {
		return t.Slot
	}
	switch {
	case len(s.free) > 0:
		last := len(s.free) - 1
		t.Slot = s.free[last]
		s.free = s.free[:last]
	case s.next < s.limit:
		t.Slot = s.next
		s.next++
	}
	return t.Slot
}

func (s *CorePublisher) Release(p *Policy, t *Task) {
	if t.Slot == 0 {
		return
	}
	if io, ok := p.io.(publishIO); ok {
		io.Publish(t.Slot, TaskPub{})
	}
	s.free = append(s.free, t.Slot)
	t.Slot = 0
}

func (p *Policy) coreVtime(t *Task, now uint64) (uint64, bool) {
	pk := t.pack
	if pk == nil {
		return 0, false
	}
	gen := pk.emptyGen + 1
	if pk.core.gen != gen {
		pk.core.origin, pk.core.gen = pk.Ref, gen
	}
	v := t.Vruntime
	// BPF projects currently running tasks after publication using run_start;
	// the published coordinate is the last event-time snapshot.
	return v - pk.core.origin, true
}

func (s *CorePublisher) PublishTask(p *Policy, t *Task, now uint64) {
	if t.Slot == 0 {
		return
	}
	io, ok := p.io.(publishIO)
	if !ok {
		return
	}
	pub := TaskPub{Pid: t.Pid, Vruntime: t.Vruntime, Deadline: t.Req.Deadline,
		Lag: t.lag, Weight: t.Weight}
	if v, ok := p.coreVtime(t, now); ok {
		pub.CoreVtime = v
	}
	io.Publish(t.Slot, pub)
}
