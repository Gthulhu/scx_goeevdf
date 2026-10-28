package eevdf

// shouldPreempt implements fair.c's wakeup_preempt_entity and RUN_TO_PARITY.
func (p *Policy) shouldPreempt(w *Task, cpu int32) bool {
	return p.shouldPreemptWith(w, cpu, DefaultFeatures())
}

func (p *Policy) shouldPreemptWith(w *Task, cpu int32, f Features) bool {
	if !f.WakeupPreempt {
		return false
	}
	cur := p.cpus[cpu].task
	if cur == nil || cur == w {
		return false
	}
	if p.schedIdle(cur) && !p.schedIdle(w) {
		p.shortenProtect(cpu, 0)
		return true
	}
	if p.schedIdle(w) || w.Policy == SchedBatch {
		return false
	}
	ref := p.refAt(p.packs[cpu], p.now)
	queued := w.node != nil && w.node.owner == &p.trees[cpu]
	if queued {
		p.trees[cpu].Remove(w)
	}
	head := p.trees[cpu].Min()
	if f.Eligibility && f.EligibleScan {
		head = p.trees[cpu].PickEligible(ref)
	}
	minSlice := p.trees[cpu].MinSlice()
	if queued {
		p.trees[cpu].Insert(w)
	}
	preempt := p.wakePreempts(w, cur, cpu, ref, head, f)
	if !preempt && f.RunToParity {
		slice := min(p.request(w), p.request(cur))
		if minSlice != 0 {
			slice = min(slice, minSlice)
		}
		p.shortenProtect(cpu, slice)
	}
	return preempt
}

func (p *Policy) wakePreempts(w, cur *Task, cpu int32, ref uint64, head *Task, f Features) bool {
	if f.Eligibility && TimeBefore(ref, w.Vruntime) {
		return false
	}
	if f.PreemptShort && p.request(w) < p.request(cur) {
		return true
	}
	_, delta := p.uncharged(cpu, p.now)
	current := p.currVruntimeAt(cpu, p.now)
	owed := f.Eligibility && delta < p.request(cur) && !TimeBefore(ref, current)
	if f.RunToParity && owed && TimeBefore(current, p.cpus[cpu].protect) {
		return false
	}
	if (owed || !f.Eligibility) && !TimeBefore(w.Req.Deadline, cur.Req.Deadline) {
		return false
	}
	return head == nil || TimeBefore(w.Req.Deadline, head.Req.Deadline)
}

func (p *Policy) shortenProtect(cpu int32, slice uint64) {
	cur := p.cpus[cpu].task
	limit := p.currVruntimeAt(cpu, p.now) + slice*Nice0Weight/max(cur.Weight, 1)
	if TimeBefore(limit, p.cpus[cpu].protect) {
		p.cpus[cpu].protect = limit
	}
}

func (p *Policy) schedIdle(t *Task) bool {
	if t.Policy == SchedIdle {
		return true
	}
	for g := p.group(t); g != nil; g = g.Parent {
		if g.Idle {
			return true
		}
	}
	return false
}
