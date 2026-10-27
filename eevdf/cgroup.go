package eevdf

import "fmt"

// Group is a configured cgroup v2 CPU controller. Unknown IDs run at root
// weight until their configuration is supplied.
type Group struct {
	ID     uint64
	Parent *Group
	Weight uint64 // cpu.weight, 1..10000
	queues []Queue
	bw     *bwGroup
}

// Queue represents a group's runnable load on one CPU.
type Queue struct {
	load, shares uint64
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
	if g == nil {
		g = &Group{ID: id, queues: make([]Queue, len(p.cpus))}
		p.groups[id] = g
	}
	g.Parent, g.Weight = parent, weight
	for c := range p.cpus {
		p.recomputeWeights(int32(c))
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
	for c := range p.cpus {
		p.recomputeWeights(int32(c))
	}
	if g.bw != nil {
		for len(g.bw.backlog) != 0 {
			t := g.bw.backlog[0]
			g.bw.backlog = g.bw.backlog[1:]
			t.backlog = nil
			p.restore(t)
		}
	}
}

func (p *Policy) group(t *Task) *Group { return p.groups[t.Cgid] }

func (p *Policy) recomputeWeights(cpu int32) {
	pk := p.packs[cpu]
	// Compute each group's queue load from its direct tasks and active child
	// entities. A child contributes its shares once, not once per task.
	for _, g := range p.groups {
		g.queues[cpu] = Queue{shares: cgroupLoadWeight(g.Weight)}
	}
	members := make([]*Task, 0, len(pk.members))
	for _, t := range pk.members {
		members = append(members, t)
	}
	for _, t := range members {
		if g := p.group(t); g != nil {
			g.queues[cpu].load += t.Nice
		}
	}
	// Bottom-up by ancestry depth; a queue with load contributes one entity
	// at the parent, regardless of its task count.
	for depth := len(p.groups); depth > 0; depth-- {
		for _, g := range p.groups {
			if g.Parent == nil || groupDepth(g) != depth || g.queues[cpu].load == 0 {
				continue
			}
			g.Parent.queues[cpu].load += g.queues[cpu].shares
		}
	}
	for _, t := range members {
		w := t.Nice
		if g := p.group(t); g != nil {
			fixed := t.Nice << GroupWeightShift
			for ; g != nil; g = g.Parent {
				q := g.queues[cpu]
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
