package eevdf

import "math/rand/v2"

const migrationCostNs uint64 = 500_000

type nbCPU struct {
	avgIdle, maxCost, idleStamp uint64
	cost, costAt [3]uint64
	calls, succ, statsAt, ratio [3]uint64
	failed uint64
	cursor int
}

type Newidle struct {
	p *Policy
	cpus []nbCPU
	overloaded int
	rng *rand.Rand
}

func newNewidle(p *Policy) *Newidle {
	n := &Newidle{p: p, cpus: make([]nbCPU, len(p.cpus)),
		rng: rand.New(rand.NewPCG(1, 2))}
	for i := range n.cpus {
		n.cpus[i].avgIdle = 2*migrationCostNs
		n.cpus[i].maxCost = migrationCostNs
		n.cpus[i].ratio = [3]uint64{1024, 1024, 1024}
	}
	return n
}

func (n *Newidle) taskHot(t *Task, src, dst int32, now uint64) bool {
	if n.p.topo.SMT && n.p.topo.sameCore(src, dst) {
		return false
	}
	stamp := n.p.lastStop[t]
	return stamp != 0 && now >= stamp && now-stamp < migrationCostNs
}

// scan searches donor runqueues in round-robin order, preserving deadline
// order within each queue and never detaching a task pinned away from dst.
func (n *Newidle) scan(dst int32, span []int32, lv int, now uint64, force bool) (*Task, bool) {
	st := &n.cpus[dst]
	saw := false
	for i := range span {
		index := (st.cursor+i)%len(span)
		src := span[index]
		tr := &n.p.trees[src]
		if tr.Len() == 0 { continue }
		saw = true
		t := tr.firstN(taskScan, func(t *Task) bool {
			return n.p.allowed(t, dst) && !n.p.throttled(t) &&
				(force || st.failed > uint64(lv)+1 || !n.taskHot(t, src, dst, now))
		})
		if t == nil { continue }
		st.cursor = (index+1)%len(span)
		n.p.moveQueued(t, dst, false)
		return t, true
	}
	return nil, saw
}

func (n *Newidle) pull(dst int32, now uint64, force bool) *Task {
	if n.overloaded == 0 { return nil }
	st := &n.cpus[dst]
	if !force && st.avgIdle < st.cost[lvLLC] { return nil }
	var spent uint64
	for lv, span := range n.p.topo.newidleSpans(dst) {
		if len(span) == 0 { continue }
		if !force && lv != lvLLC && st.avgIdle < spent+st.cost[lv] { break }
		weight := uint64(1)
		if !force {
			w, ok := newidleSample(st.ratio[lv], n.rng.Uint32())
			if !ok { continue }
			weight = w
		}
		// The policy clock represents event time; scanning cost is measured
		// against it only when the caller provides a progressing clock.
		t, saw := n.scan(dst, span, lv, now, force)
		st.cost[lv], st.costAt[lv] = newidleCostUpdate(st.cost[lv], st.costAt[lv], spent, now)
		if !force {
			st.calls[lv]++
			if t != nil { st.succ[lv] += weight }
			st.calls[lv], st.succ[lv], st.statsAt[lv], st.ratio[lv] =
				newidleStatsFold(st.calls[lv], st.succ[lv], st.statsAt[lv], st.ratio[lv], now)
		}
		if t != nil {
			st.failed = 0
			return t
		}
		if saw { st.failed++ }
	}
	return nil
}

func (n *Newidle) idleEnded(c int32, now uint64) {
	st := &n.cpus[c]
	if st.idleStamp == 0 { return }
	if now >= st.idleStamp {
		st.avgIdle = avgIdleUpdate(st.avgIdle, now-st.idleStamp, st.maxCost)
	}
	st.idleStamp = 0
}
func (n *Newidle) decay(now uint64) {
	for i := range n.cpus {
		st := &n.cpus[i]
		var total uint64
		for lv := range st.cost {
			st.cost[lv], st.costAt[lv] = newidleCostUpdate(st.cost[lv], st.costAt[lv], 0, now)
			total += st.cost[lv]
		}
		st.maxCost = max(total, migrationCostNs)
	}
}

// pull is retained as the policy's idle handoff entry point.
func (p *Policy) pull(dst int32) {
	p.nb.cpus[dst].idleStamp = p.now
	if t := p.nb.pull(dst, p.now, p.act.cpus[dst].reserved); t != nil {
		p.dispatch(t, dst, false)
	} else {
		p.act.newidleRequest(dst, p.now)
	}
}
