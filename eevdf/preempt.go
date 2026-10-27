package eevdf

// shouldPreempt implements fair.c's wakeup_preempt_entity and RUN_TO_PARITY.
func (p *Policy) shouldPreempt(w *Task, cpu int32) bool {
	cur := p.cpus[cpu].task
	if cur == nil || cur == w {
		return false
	}
	if cur.Policy == SchedIdle && w.Policy != SchedIdle {
		return true
	}
	if w.Policy == SchedIdle || w.Policy == SchedBatch {
		return false
	}
	ref := p.packs[cpu].Ref
	if TimeBefore(ref, w.Vruntime) {
		return false
	}
	if p.request(w) < p.request(cur) {
		return true
	}
	if TimeBefore(cur.Vruntime, p.cpus[cpu].protect) && !TimeBefore(ref, cur.Vruntime) {
		return false
	}
	if !TimeBefore(w.Req.Deadline, cur.Req.Deadline) {
		return false
	}
	p.trees[cpu].Remove(w)
	head := p.trees[cpu].PickEligible(ref)
	p.trees[cpu].Insert(w)
	return head == nil || TimeBefore(w.Req.Deadline, head.Req.Deadline)
}
