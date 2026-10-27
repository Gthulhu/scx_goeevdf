package eevdf

const (
	hrtickMinNs uint64 = 10_000
	CmdHrtick   uint32 = 4
	CmdfHrtick  uint32 = 1
)

type nowIO interface{ Now() uint64 }

// Hrtick tracks the earliest pending timer for each CPU. The BPF timer checks
// run_seq, so a timer from a previous run can safely expire without kicking it.
type Hrtick struct {
	p       *Policy
	at      []uint64
	Enabled bool
}

func NewHrtick(p *Policy) *Hrtick {
	return &Hrtick{p: p, at: make([]uint64, len(p.cpus)), Enabled: true}
}

func (h *Hrtick) start(c int32, now uint64) {
	p := h.p
	if !h.Enabled || !p.valid(c) || p.cpus[c].task == nil ||
		p.trees[c].Len() == 0 || p.cpus[c].task.Weight == 0 {
		return
	}
	cur := p.cpus[c].task
	remaining := int64(cur.Req.Deadline - cur.Vruntime)
	if remaining <= 0 {
		return
	}
	in := uint64(remaining) * cur.Weight / Nice0Weight
	if in < hrtickMinNs {
		return
	}
	when := now + in
	if h.at[c] != 0 && TimeBefore(now, h.at[c]) && !TimeBefore(when, h.at[c]) {
		return
	}
	r, ok := p.io.(runSeqIO)
	if !ok {
		return
	}
	if err := p.io.Submit(Cmd{Kind: CmdHrtick, Cpu: c, Slice: when, Slot: uint32(r.RunSeq(c))}); err == nil {
		h.at[c] = when
	}
}

// joinedLone arms a timer when the first competitor joins a running task.
func (h *Hrtick) joinedLone(c int32, now uint64) {
	if h.p.valid(c) && h.p.trees[c].Len() == 1 {
		h.start(c, now)
	}
}

// dispatchFlag is set only when a task will have a waiting competitor after
// dispatch. BPF arms that timer from running(), using the actual start time.
func (h *Hrtick) dispatchFlag(c int32) uint32 {
	if h.Enabled && h.p.valid(c) && h.p.trees[c].Len() > 0 {
		return CmdfHrtick
	}
	return 0
}

func (h *Hrtick) dispatched(c int32, now, slice uint64, flags uint32) {
	if !h.p.valid(c) {
		return
	}
	h.at[c] = 0
	if flags&CmdfHrtick != 0 {
		h.at[c] = now + slice
	}
}
