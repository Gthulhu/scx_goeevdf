package eevdf

import "fmt"

// bwGroup tracks one finite cpu.max quota in nanoseconds.
type bwGroup struct {
	group      *Group
	quota      uint64
	period     uint64
	pool       uint64
	nextRefill uint64
	local      []uint64
	exhausted  []bool
	backlog    []*Task
}

// SetCPUQuota configures cpu.max for a previously registered group. A zero
// quota disables the limit; a finite quota requires a nonzero period.
func (p *Policy) SetCPUQuota(id, quota, period uint64) error {
	g := p.groups[id]
	if g == nil || (quota != 0 && period == 0) {
		return fmt.Errorf("invalid cgroup or cpu.max period")
	}
	old := gBandwidth(g)
	if old != nil {
		p.bandwidth = removeBandwidth(p.bandwidth, old)
	}
	if quota == 0 {
		g.bw = nil
		if old != nil {
			for len(old.backlog) != 0 {
				t := old.backlog[0]
				old.backlog = old.backlog[1:]
				t.backlog = nil
				p.restore(t)
			}
		}
		return nil
	}
	bw := &bwGroup{group: g, quota: quota, period: period, pool: quota,
		local: make([]uint64, len(p.cpus)), exhausted: make([]bool, len(p.cpus))}
	if old != nil {
		bw.backlog = old.backlog
		for _, t := range bw.backlog {
			t.backlog = bw
		}
	}
	g.bw = bw
	p.bandwidth = append(p.bandwidth, bw)
	p.unpark()
	return nil
}

func gBandwidth(g *Group) *bwGroup { return g.bw }

func removeBandwidth(groups []*bwGroup, target *bwGroup) []*bwGroup {
	for i, g := range groups {
		if g == target {
			return append(groups[:i], groups[i+1:]...)
		}
	}
	return groups
}

func (p *Policy) throttled(t *Task) bool {
	for g := p.group(t); g != nil; g = g.Parent {
		if b := g.bw; b != nil && (b.exhausted[t.Cpu] ||
			(b.local[t.Cpu] == 0 && b.pool == 0)) {
			return true
		}
	}
	return false
}

func (p *Policy) capSlice(t *Task, slice uint64) uint64 {
	for g := p.group(t); g != nil; g = g.Parent {
		if b := g.bw; b != nil {
			if b.exhausted[t.Cpu] {
				return 0
			}
			slice = min(slice, b.local[t.Cpu]+min(b.pool, uint64(5_000_000)))
		}
	}
	return slice
}

// reserve transfers dispatch budget from each ancestor pool to its CPU.
// Budgets are reserved before another CPU can dispatch against the same pool.
func (p *Policy) reserve(t *Task, slice uint64) {
	for g := p.group(t); g != nil; g = g.Parent {
		if b := g.bw; b != nil && b.local[t.Cpu] < slice {
			take := slice - b.local[t.Cpu]
			b.pool -= take
			b.local[t.Cpu] += take
		}
	}
}

// Accounting arrives on events, not while the task runs. Dispatch slices
// bound normal runtime, but an overrun cannot be stopped through this IO.
func (p *Policy) chargeBandwidth(t *Task, delta uint64) bool {
	if t.state != Dispatched {
		return false
	}
	for g := p.group(t); g != nil; g = g.Parent {
		b := g.bw
		if b == nil {
			continue
		}
		used := min(delta, b.local[t.Cpu])
		b.local[t.Cpu] -= used
		remaining := delta - used
		if remaining != 0 {
			take := min(remaining, b.pool)
			b.pool -= take
			if take < remaining {
				b.exhausted[t.Cpu] = true
			}
		}
	}
	return p.throttled(t)
}

func (p *Policy) parkThrottled() {
	for _, pk := range p.packs {
		members := make([]*Task, 0, len(pk.members))
		for _, t := range pk.members {
			if t.state == Queued && p.throttled(t) {
				members = append(members, t)
			}
		}
		for _, t := range members {
			p.park(t)
		}
	}
}

func (p *Policy) removeBacklog(t *Task) {
	b := t.backlog
	if b == nil {
		return
	}
	for i, other := range b.backlog {
		if other == t {
			b.backlog = append(b.backlog[:i], b.backlog[i+1:]...)
			break
		}
	}
	t.backlog = nil
}

func (p *Policy) park(t *Task) {
	if t.backlog != nil {
		return
	}
	p.removeQueued(t)
	if t.pack != nil {
		t.pack.quiesce(t, false, max(p.request(t), p.Slice)+p.TickNs)
		p.recomputeWeights(t.Cpu)
	}
	for g := p.group(t); g != nil; g = g.Parent {
		if b := g.bw; b != nil && (b.exhausted[t.Cpu] ||
			(b.local[t.Cpu] == 0 && b.pool == 0)) {
			b.backlog = append(b.backlog, t)
			t.backlog, t.state = b, Parked
			return
		}
	}
	// A quota may have changed between the availability check and parking.
	p.queue(t, t.Cpu, false)
}

func (p *Policy) unpark() {
	for _, b := range p.bandwidth {
		for len(b.backlog) != 0 {
			t := b.backlog[0]
			if p.throttled(t) {
				break
			}
			b.backlog = b.backlog[1:]
			t.backlog = nil
			p.restore(t)
		}
	}
}

func (p *Policy) restore(t *Task) {
	if p.throttled(t) {
		p.park(t)
		return
	}
	p.packs[t.Cpu].place(t, false)
	p.recomputeWeights(t.Cpu)
	p.prepare(t)
	p.queue(t, t.Cpu, false)
}

// HasBacklog reports whether a cpu.max-throttled runnable task needs a
// future Periodic call even when the kernel produces no scheduling events.
func (p *Policy) HasBacklog() bool {
	for _, b := range p.bandwidth {
		if len(b.backlog) != 0 {
			return true
		}
	}
	return false
}

// Pending reports work that needs the scheduler loop to run again, including
// queued dispatch retries and quota-throttled runnable tasks.
func (p *Policy) Pending() bool {
	if p.HasBacklog() {
		return true
	}
	for i := range p.trees {
		if p.trees[i].Len() != 0 {
			return true
		}
	}
	return false
}

// Periodic advances all configured cpu.max periods and releases runnable
// backlog. Call it with monotonic nanoseconds on the scheduling goroutine.
func (p *Policy) Periodic(now uint64) {
	for _, b := range p.bandwidth {
		if b.nextRefill == 0 {
			b.nextRefill = now + b.period
			continue
		}
		if TimeBefore(now, b.nextRefill) {
			continue
		}
		periods := (now-b.nextRefill)/b.period + 1
		b.nextRefill += periods * b.period
		b.pool = b.quota
		for cpu := range b.local {
			b.local[cpu] = 0
			b.exhausted[cpu] = false
		}
	}
	p.unpark()
}
