package eevdf

const imbalancePct uint64 = 117

type balCPU struct {
	interval, next, failed [3]uint64
}
type Balance struct {
	p *Policy
	cpus []balCPU
}

func sisBudget(nr, utilSum uint64) uint64 {
	if nr == 0 { return 0 }
	x := min(utilSum/nr, 1024)
	scaled := min(x*x*imbalancePct*imbalancePct/(10000*1024), 1024)
	return nr*(1024-scaled)/1024
}

// groupKey gives each domain the correct child grouping: cores inside an
// LLC, caches inside a node, and nodes across the machine.
func (b *Balance) groupKey(level int, cpu int32) int {
	t := b.p.topo
	switch level {
	case lvLLC: return t.Core[cpu]
	case lvNode: return t.LLC[cpu]
	default: return t.Node[cpu]
	}
}
func (b *Balance) domainKey(level int, cpu int32) int {
	switch level {
	case lvLLC: return b.p.topo.LLC[cpu]
	case lvNode: return b.p.topo.Node[cpu]
	default: return 0
	}
}
func (b *Balance) capacity(c int32) uint64 {
	if provider, ok := b.p.io.(CapacityIO); ok {
		return max(b.p.topo.Capacity[c]*provider.Available(c)/1024, 1)
	}
	return b.p.topo.Capacity[c]
}
func (b *Balance) load(c int32, now uint64) uint64 {
	// PELT is slow to rise after the first enqueue. The instantaneous pack
	// weight makes balancing work before the first full half-life as well.
	return max(b.p.ld.cpuLoad(c, now), b.p.packs[c].W)
}
func (b *Balance) interval(level, count int) uint64 {
	factor := uint64(16)
	if level == lvLLC { factor = 4 }
	return max(uint64(count)*factor, 1)
}

func (b *Balance) run(now uint64) {
	p := b.p
	for lv := lvLLC; lv <= lvSystem; lv++ {
		seen := map[[2]int]bool{}
		for c := range p.cpus {
			cpu := int32(c)
			domain, group := b.domainKey(lv, cpu), b.groupKey(lv, cpu)
			key := [2]int{domain, group}
			if seen[key] { continue }
			seen[key] = true
			var members []int32
			for other := range p.cpus {
				o := int32(other)
				if b.domainKey(lv, o) == domain { members = append(members, o) }
			}
			if len(members) < 2 { continue }
			leader := cpu
			for _, o := range members {
				if b.groupKey(lv, o) == group && b.load(o, now) < b.load(leader, now) {
					leader = o
				}
			}
			st := &b.cpus[leader]
			minInterval := b.interval(lv, len(members))
			if st.interval[lv] == 0 {
				st.interval[lv] = minInterval
				st.next[lv] = now + (1+uint64(leader)%minInterval)*1_000_000
				continue
			}
			if now < st.next[lv] { continue }
			moved := b.balanceDomain(lv, group, leader, members, now)
			if moved {
				st.failed[lv] = 0
				st.interval[lv] = minInterval
			} else {
				st.failed[lv]++
				st.interval[lv] = min(st.interval[lv]*2, 2*minInterval)
			}
			st.next[lv] = now + st.interval[lv]*1_000_000
		}
	}
}

func (b *Balance) balanceDomain(lv, local int, leader int32, members []int32, now uint64) bool {
	p := b.p
	var total, capacity, localLoad, localCapacity uint64
	var localCPUs []int32
	for _, c := range members {
		load, cap := b.load(c, now), b.capacity(c)
		total += load
		capacity += cap
		if b.groupKey(lv, c) == local {
			localLoad += load
			localCapacity += cap
			localCPUs = append(localCPUs, c)
		}
	}
	if capacity == 0 { return false }
	avg := total*1024/capacity
	fair := avg*localCapacity/1024
	if localLoad >= fair { return false }
	room := fair-localLoad
	// Compare normalized group loads, then pick the heaviest donor with a
	// queued, affinity-compatible task. A group with only running work is
	// left to active balancing.
	var src int32 = -1
	var excess uint64
	for _, c := range members {
		if b.groupKey(lv, c) == local || p.trees[c].Len() == 0 { continue }
		group := b.groupKey(lv, c)
		var gl, gc uint64
		for _, x := range members {
			if b.groupKey(lv, x) == group {
				gl += b.load(x, now)
				gc += b.capacity(x)
			}
		}
		need := avg*gc/1024
		if gl <= need || gl-need <= excess { continue }
		excess, src = gl-need, c
	}
	if src < 0 { return false }
	budget := min(room, excess)
	for _, dst := range localCPUs {
		if dst != leader && p.cpus[dst].task != nil && p.trees[dst].Len() != 0 { continue }
		task := p.trees[src].firstN(taskScan, func(t *Task) bool {
			weight := t.Weight
			if p.allowed(t, dst) && !p.throttled(t) &&
				(weight>>min(b.cpus[leader].failed[lv], 63)) <= budget {
				return b.cpus[leader].failed[lv] > 1 || !p.nb.taskHot(t, src, dst, now)
			}
			return false
		})
		if task == nil { continue }
		p.moveQueued(task, dst, true)
		return true
	}
	return false
}

// moveQueued preserves remaining service and lag while transferring a task
// between runqueues. It is also used by the idle pull path.
func (p *Policy) moveQueued(t *Task, dst int32, dispatchIfIdle bool) {
	src := t.Cpu
	p.removeQueued(t)
	t.state = Sleeping
	p.packs[dst].place(t, false)
	t.Cpu = dst
	p.recomputeWeights(src)
	p.recomputeWeights(dst)
	p.queueRetry(t, dst)
	if dispatchIfIdle && p.cpus[dst].task == nil && p.io.Idle(dst) {
		p.dispatch(t, dst, false)
	}
}
