package eevdf

import "fmt"

// Group is a configured cgroup v2 CPU controller. Unknown IDs run at root
// weight until their configuration is supplied.
type Group struct {
	ID     uint64
	Parent *Group
	Weight uint64 // cpu.weight, 1..10000
	Idle   bool   // cpu.idle; overrides Weight without discarding it
	queues []Queue
	bw     *bwGroup
	root   bool
}

// Queue represents a group's runnable load on one CPU.
type Queue struct {
	load, nr, shares uint64
}

// SetCgroup configures cpu.weight and ancestry. Parents must be configured
// before children; parentID 0 denotes the root cgroup.
func (p *Policy) SetCgroup(id, parentID, weight uint64) error {
	if id == 0 || id == parentID || weight < 1 || weight > 10000 {
		return fmt.Errorf("invalid cgroup id, parent or cpu.weight")
	}
	var parent *Group
	if parentID != 0 {
		parent = p.groups[parentID]
		if parent == nil {
			return fmt.Errorf("cgroup parent %d not configured", parentID)
		}
		for g := parent; g != nil; g = g.Parent {
			if g.ID == id {
				return fmt.Errorf("cgroup ancestry cycle")
			}
		}
	}
	g := p.groups[id]
	if g != nil && g.root && parent != nil {
		return fmt.Errorf("cannot reparent root cgroup")
	}
	if g == nil {
		g = &Group{ID: id, queues: make([]Queue, len(p.cpus))}
		p.groups[id] = g
	}
	g.Parent, g.Weight = parent, weight
	p.recomputeWeights(0)
	return nil
}

// SetCgroupRoot registers the actual cgroupfs root, whose tasks retain their
// nice weights. SetCgroup(id, 0, weight) instead registers a weighted top-level
// group. The root remains in the ancestry for bandwidth accounting.
func (p *Policy) SetCgroupRoot(id uint64) error {
	if id == 0 {
		return fmt.Errorf("invalid root cgroup id")
	}
	for otherID, g := range p.groups {
		if g.root && otherID != id {
			return fmt.Errorf("root cgroup already configured")
		}
	}
	g := p.groups[id]
	if g != nil && g.Parent != nil {
		return fmt.Errorf("root cgroup has a parent")
	}
	if g == nil {
		g = &Group{ID: id, Weight: 100, queues: make([]Queue, len(p.cpus))}
		p.groups[id] = g
	}
	g.root, g.Idle = true, false
	p.recomputeWeights(0)
	return nil
}

// SetCgroupIdle updates cpu.idle on an already configured group. Idle groups
// use WEIGHT_IDLEPRIO (3), independently of their configured cpu.weight.
func (p *Policy) SetCgroupIdle(id uint64, idle bool) error {
	g := p.groups[id]
	if g == nil {
		return fmt.Errorf("cgroup %d not configured", id)
	}
	if g.root && idle {
		return fmt.Errorf("root cgroup cannot be idle")
	}
	if g.Idle != idle {
		g.Idle = idle
		p.recomputeWeights(0)
	}
	return nil
}

// RemoveCgroup removes a group after cgroupfs deletion. Active tasks whose ID
// was removed run as root tasks; children inherit its former parent.
func (p *Policy) RemoveCgroup(id uint64) {
	g := p.groups[id]
	if g == nil {
		return
	}
	if g.bw != nil {
		p.bandwidth = removeBandwidth(p.bandwidth, g.bw)
	}
	delete(p.groups, id)
	for _, child := range p.groups {
		if child.Parent == g {
			child.Parent = g.Parent
		}
	}
	p.recomputeWeights(0)
	if g.bw != nil {
		for len(g.bw.backlog) != 0 {
			t := g.bw.backlog[0]
			g.bw.backlog = g.bw.backlog[1:]
			t.backlog = nil
			p.restore(t)
		}
	}
}

func (p *Policy) group(t *Task) *Group {
	if !p.F.Cgroups {
		return nil
	}
	return p.groups[t.Cgid]
}

func (p *Policy) recomputeWeights(_ int32) {
	// Shares depend on every CPU's load, so a local membership change must
	// also refresh remote tasks. These are instantaneous owner-local loads,
	// not PELT averages or a cross-owner shared load estimate.
	for _, g := range p.groups {
		clear(g.queues)
	}
	members := make([]*Task, 0, len(p.tasks))
	for cpu, pk := range p.packs {
		for _, t := range pk.members {
			members = append(members, t)
			if g := p.group(t); g != nil && !g.root {
				g.queues[cpu].load += t.Nice
				g.queues[cpu].nr++
			}
		}
	}
	// Finish children before parents. Each active child queue contributes
	// exactly one entity, weighted by its newly computed local shares.
	maxDepth := 0
	for _, g := range p.groups {
		maxDepth = max(maxDepth, groupDepth(g))
	}
	for depth := maxDepth; depth > 0; depth-- {
		for _, g := range p.groups {
			if g.root || groupDepth(g) != depth {
				continue
			}
			var load, nr uint64
			for _, q := range g.queues {
				load += q.load
				nr += q.nr
			}
			weight := cgroupLoadWeight(g.Weight)
			if g.Idle {
				weight = 3
			}
			for cpu := range g.queues {
				q := &g.queues[cpu]
				q.shares = groupShares(weight, nr, uint64(len(p.cpus)),
					q.load, q.load, load, q.load)
				if q.nr != 0 && g.Parent != nil && !g.Parent.root {
					g.Parent.queues[cpu].load += q.shares
					g.Parent.queues[cpu].nr++
				}
			}
		}
	}
	for _, t := range members {
		w := t.Nice
		if g := p.group(t); g != nil {
			fixed := t.Nice << GroupWeightShift
			for ; g != nil && !g.root; g = g.Parent {
				q := g.queues[t.pack.CPU]
				fixed = propWeight(fixed, q.shares, q.load)
			}
			w = roundWeight(fixed)
		}
		if w != t.Weight {
			p.reweight(t, w)
		}
	}
}

func groupDepth(g *Group) int {
	depth := 0
	for ; g != nil; g = g.Parent {
		depth++
	}
	return depth
}

func (p *Policy) reweight(t *Task, w uint64) {
	queued := t.state == Queued
	if queued {
		p.removeQueued(t)
	}
	pk := t.pack
	ref := pk.Ref
	lag := pk.lag(t, max(p.request(t), p.Slice)+p.TickNs)
	lag = vdiv(lag*int64(t.Weight), w)
	pk.leave(t, false)
	t.Weight = w
	t.Vruntime = ref - uint64(lag)
	pk.join(t)
	if !t.initial {
		p.prepare(t)
	}
	if queued {
		p.trees[t.Cpu].Insert(t)
		p.syncQueued(t.Cpu)
	}
}
