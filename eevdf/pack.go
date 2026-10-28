package eevdf

// Pack implements fair.c's weighted virtual-time reference for one CPU.
type Pack struct {
	CPU       int32
	Ref, W    uint64
	remainder uint64
	emptyGen  uint64
	members   map[int32]*Task
	core      struct{ origin, gen uint64 }
}

func (pk *Pack) join(t *Task) {
	if pk.members == nil {
		pk.members = make(map[int32]*Task)
	}
	if pk.W == 0 {
		pk.Ref = t.Vruntime
	} else {
		pk.Ref += joinDelta(pk.Ref, pk.W, t.Vruntime, t.Weight)
	}
	pk.W += t.Weight
	pk.members[t.Pid] = t
	t.pack, t.joined = pk, t.Vruntime
}

func (pk *Pack) leave(t *Task, emptied bool) {
	w, sum := t.Weight, pk.W
	if sum > w {
		pk.Ref += leaveDelta(pk.Ref, sum, t.joined, w)
		pk.W -= w
	} else {
		pk.W, pk.remainder = 0, 0
		if emptied {
			pk.emptyGen++
		}
	}
	delete(pk.members, t.Pid)
	t.pack = nil
}

func (pk *Pack) fold(t *Task, distance uint64) {
	if pk.W == 0 || distance == 0 {
		return
	}
	d, r := fold(t.Weight, distance, pk.W, pk.remainder)
	pk.Ref += d
	pk.remainder = r
	t.joined += distance
}

func (pk *Pack) lag(t *Task, request uint64) int64 {
	return Lag(pk.Ref, t.Vruntime, LagLimit(request, t.Weight))
}

func (pk *Pack) settle(t *Task) int64 {
	ref := uint64(0)
	if t.delay.pack != nil {
		ref = t.delay.pack.Ref
	}
	return pk.settleAt(t, ref)
}

func (pk *Pack) settleAt(t *Task, ref uint64) int64 {
	d := t.delay
	t.delay = delayRec{}
	if d.pack == nil {
		return t.lag
	}
	if d.pack.emptyGen != d.gen {
		return 0
	}
	return delayDebt(t.lag, int64(ref-d.ref), d.sum, t.Weight)
}

func (pk *Pack) place(t *Task, sleep bool) {
	pk.placeWith(t, sleep, true, true)
}

func (pk *Pack) placeWith(t *Task, sleep, placeLag, relativeDeadline bool) {
	if t.pack == pk {
		return
	}
	lag := int64(0)
	if placeLag {
		lag = pk.settle(t)
	} else {
		t.delay = delayRec{}
	}
	if t.pack != nil {
		if placeLag {
			lag = t.pack.lag(t, max(t.Req.Slice, 1))
		}
	}
	pk.placeWithLag(t, sleep, relativeDeadline, pk.Ref, lag)
}

func (pk *Pack) placeWithLag(t *Task, sleep, relativeDeadline bool, ref uint64, lag int64) {
	if t.pack != nil {
		t.pack.leave(t, true)
	}
	target := ref
	if pk.W != 0 {
		target = place(ref, pk.W, lag, t.Weight)
	}
	if sleep || !relativeDeadline {
		t.Req.Sleep()
	} else {
		t.Req.Rebase(t.Vruntime, target)
	}
	t.Vruntime, t.lag = target, 0
	pk.join(t)
}

func (pk *Pack) quiesce(t *Task, sleep bool, request uint64) {
	pk.quiesceWith(t, sleep, request, true)
}

func (pk *Pack) quiesceWith(t *Task, sleep bool, request uint64, delayed bool) {
	if t.pack != pk {
		return
	}
	t.lag = pk.lag(t, request)
	pk.leave(t, true)
	if delayed && sleep && t.lag < 0 && pk.W != 0 {
		t.delay = delayRec{pack: pk, ref: pk.Ref, sum: pk.W, gen: pk.emptyGen}
	} else {
		t.delay = delayRec{}
	}
}
