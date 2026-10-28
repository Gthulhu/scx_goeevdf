package eevdf

func (p *Policy) uncharged(cpu int32, now uint64) (*Task, uint64) {
	cur := &p.cpus[cpu]
	t := cur.task
	if t == nil || p.io.CurrPid(cpu) != t.Pid {
		return t, 0
	}
	start, ok := p.io.(runStartIO)
	if !ok {
		return t, 0
	}
	runStart := start.RunStart(cpu)
	if runStart == 0 {
		return t, 0
	}
	since := max(runStart, cur.since)
	if since == 0 || !TimeBefore(since, now) {
		return t, 0
	}
	return t, now - since
}

func (p *Policy) currVruntimeAt(cpu int32, now uint64) uint64 {
	t, delta := p.uncharged(cpu, now)
	if t == nil {
		return 0
	}
	return advance(t.Vruntime, delta, t.Weight)
}

func (p *Policy) refAt(pk *Pack, now uint64) uint64 {
	if !p.F.VrefUpdate {
		return pk.Ref
	}
	t, delta := p.uncharged(pk.CPU, now)
	if t == nil || t.pack != pk {
		return pk.Ref
	}
	if len(pk.members) == 1 {
		return advance(t.Vruntime, delta, t.Weight)
	}
	return referenceAt(pk.Ref, pk.W, t.Weight, delta, t.Req.Slice)
}

func (p *Policy) refBeforeJoin(pk *Pack, join, now uint64) uint64 {
	if !p.F.VrefUpdate {
		return pk.Ref
	}
	t, delta := p.uncharged(pk.CPU, now)
	if t == nil || t.pack != pk {
		return pk.Ref
	}
	if len(pk.members) == 1 {
		return advance(t.Vruntime, delta, t.Weight)
	}
	return referenceBeforeJoin(pk.Ref, pk.W, t.Weight, delta, t.Req.Slice, join)
}
