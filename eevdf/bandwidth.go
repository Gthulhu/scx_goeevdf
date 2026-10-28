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
	shared     *sharedQuota
	owner      *SharedQuotaPool
	generation uint64
	pending    []uint64
	pendingGen []uint64
}

// SetCPUQuota configures cpu.max for a previously registered group. A zero
// quota disables the limit; a finite quota requires a nonzero period.
func (p *Policy) SetCPUQuota(id, quota, period uint64) error {
	g := p.groups[id]
	if g == nil || (quota != 0 && period == 0) {
		return fmt.Errorf("invalid cgroup or cpu.max period")
	}
	old := gBandwidth(g)
	if old != nil && old.shared != nil && old.quota == quota && old.period == period {
		old.owner.mu.Lock()
		same := old.shared.quota == quota && old.shared.period == period
		old.owner.mu.Unlock()
		if same {
			return nil
		}
	}
	shared := p.sharedQuota
	var quotaState *sharedQuota
	if shared != nil {
		shared.mu.Lock()
		if quota != 0 {
			quotaState = shared.groups[id]
			if quotaState == nil {
				quotaState = shared.configureLocked(id, quota, period)
			} else if quotaState.quota != quota || quotaState.period != period {
				shared.mu.Unlock()
				return fmt.Errorf("shared cpu.max for group %d differs; call pool.Configure first", id)
			}
		} else if q := shared.groups[id]; q != nil && q.quota != 0 {
			shared.mu.Unlock()
			return fmt.Errorf("disable shared cpu.max for group %d with pool.Configure first", id)
		}
		shared.mu.Unlock()
	}
	if old != nil {
		p.bandwidth = removeBandwidth(p.bandwidth, old)
	}
	if quota == 0 {
		g.bw = nil
		for c := range p.cpus {
			p.syncQueued(int32(c))
		}
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
		local: make([]uint64, len(p.cpus)), exhausted: make([]bool, len(p.cpus)),
		shared: quotaState, owner: shared, pending: make([]uint64, len(p.cpus)),
		pendingGen: make([]uint64, len(p.cpus))}
	if shared != nil {
		bw.generation = quotaState.generation
	}
	if old != nil {
		bw.backlog = old.backlog
		for _, t := range bw.backlog {
			t.backlog = bw
		}
	}
	g.bw = bw
	p.bandwidth = append(p.bandwidth, bw)
	for c := range p.cpus {
		p.syncQueued(int32(c))
	}
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
	if !p.F.CpuMax {
		return false
	}
	for g := p.group(t); g != nil; g = g.Parent {
		if b := g.bw; b != nil && b.unavailable(t.Cpu) {
			return true
		}
	}
	return false
}

func (b *bwGroup) unavailable(cpu int32) bool {
	if b.shared == nil {
		return b.exhausted[cpu] || (b.local[cpu] == 0 && b.pool == 0)
	}
	b.owner.mu.Lock()
	defer b.owner.mu.Unlock()
	b.syncLocal()
	return b.shared.quota != 0 && (b.exhausted[cpu] ||
		(b.local[cpu] == 0 && b.shared.remaining == 0))
}

// reserveSlice atomically caps and reserves every quota in the ancestry before
// a dispatch can be submitted. With shared pools, all ancestors are checked
// under one lock: no second owner can claim the same burst budget.
func (p *Policy) reserveSlice(t *Task, slice uint64) uint64 {
	if !p.F.CpuMax {
		return slice
	}
	var shared *SharedQuotaPool
	for g := p.group(t); g != nil; g = g.Parent {
		if b := g.bw; b != nil && b.owner != nil {
			shared = b.owner
			break
		}
	}
	if shared != nil {
		shared.mu.Lock()
		defer shared.mu.Unlock()
	}
	for g := p.group(t); g != nil; g = g.Parent {
		b := g.bw
		if b == nil {
			continue
		}
		remaining := b.pool
		if b.shared != nil {
			b.syncLocal()
			if b.shared.quota == 0 {
				continue
			}
			remaining = b.shared.remaining
		}
		if b.exhausted[t.Cpu] {
			return 0
		}
		if slice > b.local[t.Cpu] {
			slice = b.local[t.Cpu] + min(slice-b.local[t.Cpu], remaining, uint64(5_000_000))
		}
	}
	if slice == 0 {
		return 0
	}
	for g := p.group(t); g != nil; g = g.Parent {
		b := g.bw
		if b == nil || (b.shared != nil && b.shared.quota == 0) {
			continue
		}
		take := uint64(0)
		if b.local[t.Cpu] < slice {
			take = slice - b.local[t.Cpu]
			if b.shared != nil {
				b.shared.remaining -= take
			} else {
				b.pool -= take
			}
			b.local[t.Cpu] += take
		}
		b.pending[t.Cpu] = take
		if b.shared != nil {
			b.pendingGen[t.Cpu] = b.shared.generation
		}
	}
	return slice
}

// refundSlice returns only budget borrowed for this failed dispatch, not
// previously reserved CPU-local budget or budget from a prior period.
func (p *Policy) refundSlice(t *Task, slice uint64) {
	if !p.F.CpuMax || slice == 0 {
		return
	}
	for g := p.group(t); g != nil; g = g.Parent {
		b := g.bw
		if b == nil {
			continue
		}
		if b.shared != nil {
			b.owner.mu.Lock()
			if b.pendingGen[t.Cpu] == b.shared.generation {
				take := b.pending[t.Cpu]
				b.local[t.Cpu] -= take
				b.shared.remaining += take
			}
			b.pending[t.Cpu] = 0
			b.owner.mu.Unlock()
		} else {
			take := b.pending[t.Cpu]
			b.local[t.Cpu] -= take
			b.pool += take
			b.pending[t.Cpu] = 0
		}
	}
}

// Accounting arrives on events, not while the task runs. Dispatch slices
// bound normal runtime, but an overrun cannot be stopped through this IO.
func (p *Policy) chargeBandwidth(t *Task, delta uint64) bool {
	if !p.F.CpuMax || t.state != Dispatched {
		return false
	}
	for g := p.group(t); g != nil; g = g.Parent {
		b := g.bw
		if b == nil {
			continue
		}
		if b.shared != nil {
			b.owner.mu.Lock()
			b.syncLocal()
			if b.shared.quota != 0 {
				used := min(delta, b.local[t.Cpu])
				b.local[t.Cpu] -= used
				remaining := delta - used
				take := min(remaining, b.shared.remaining)
				b.shared.remaining -= take
				if take < remaining {
					b.exhausted[t.Cpu] = true
				}
			}
			b.owner.mu.Unlock()
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
		if b := g.bw; b != nil && b.unavailable(t.Cpu) {
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
	p.placeTask(t, t.Cpu, false)
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
		if b.shared != nil {
			continue // the single refiller owns shared epochs
		}
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
