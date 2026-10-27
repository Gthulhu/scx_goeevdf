package eevdf

func (p *Policy) firstAllowed(n *node, dst int32, ref uint64, eligible bool) *Task {
	if n == nil || eligible && TimeBefore(ref, n.minV) {
		return nil
	}
	if t := p.firstAllowed(n.left, dst, ref, eligible); t != nil {
		return t
	}
	if p.allowed(n.t, dst) && (!eligible || !TimeBefore(ref, n.t.Vruntime)) {
		return n.t
	}
	return p.firstAllowed(n.right, dst, ref, eligible)
}

// pull is sched_balance_newidle: move a queued task to a vacant allowed CPU.
func (p *Policy) pull(dst int32) {
	for c := range p.cpus {
		src := int32(c)
		if src == dst || p.trees[src].Len() == 0 {
			continue
		}
		ref := p.packs[src].Ref
		t := p.firstAllowed(p.trees[src].root, dst, ref, true)
		if t == nil {
			t = p.firstAllowed(p.trees[src].root, dst, ref, false)
		}
		if t != nil {
			p.removeQueued(t)
			t.state = Sleeping
			p.packs[dst].place(t, false)
			t.Cpu = dst
			p.recomputeWeights(src)
			p.recomputeWeights(dst)
			p.dispatch(t, dst, false)
			return
		}
	}
}
