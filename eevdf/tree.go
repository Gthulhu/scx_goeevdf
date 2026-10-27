package eevdf

import "math/rand/v2"

type node struct {
	t           *Task
	owner       *Tree
	prio, order uint64
	left, right *node
	minV        uint64
	minSlice    uint64
	size        int
}

type Tree struct {
	root *node
	next uint64
}

func less(a, b *Task) bool {
	if a.Req.Deadline != b.Req.Deadline {
		return TimeBefore(a.Req.Deadline, b.Req.Deadline)
	}
	return a.seq < b.seq
}

func lessNode(a, b *node) bool {
	if a.t.Req.Deadline != b.t.Req.Deadline || a.t.seq != b.t.seq {
		return less(a.t, b.t)
	}
	return a.order < b.order
}

func (n *node) update() {
	n.minV, n.minSlice, n.size = n.t.Vruntime, n.t.Req.Slice, 1
	for _, c := range [2]*node{n.left, n.right} {
		if c == nil {
			continue
		}
		if TimeBefore(c.minV, n.minV) {
			n.minV = c.minV
		}
		n.minSlice = min(n.minSlice, c.minSlice)
		n.size += c.size
	}
}

func rotR(n *node) *node {
	l := n.left
	n.left = l.right
	l.right = n
	n.update()
	l.update()
	return l
}

func rotL(n *node) *node {
	r := n.right
	n.right = r.left
	r.left = n
	n.update()
	r.update()
	return r
}

func insert(n, x *node) *node {
	if n == nil {
		x.update()
		return x
	}
	if lessNode(x, n) {
		n.left = insert(n.left, x)
		if n.left.prio > n.prio {
			return rotR(n)
		}
	} else {
		n.right = insert(n.right, x)
		if n.right.prio > n.prio {
			return rotL(n)
		}
	}
	n.update()
	return n
}

func remove(n, x *node) *node {
	if n == nil {
		return nil
	}
	switch {
	case n == x:
		if n.left == nil {
			return n.right
		}
		if n.right == nil {
			return n.left
		}
		if n.left.prio > n.right.prio {
			n = rotR(n)
			n.right = remove(n.right, x)
		} else {
			n = rotL(n)
			n.left = remove(n.left, x)
		}
	case lessNode(x, n):
		n.left = remove(n.left, x)
	default:
		n.right = remove(n.right, x)
	}
	n.update()
	return n
}

func (tr *Tree) Insert(t *Task) {
	if t == nil || t.node != nil {
		return
	}
	tr.next++
	t.node = &node{t: t, owner: tr, prio: rand.Uint64(), order: tr.next}
	tr.root = insert(tr.root, t.node)
}

func (tr *Tree) Remove(t *Task) {
	if t == nil || t.node == nil || t.node.owner != tr {
		return
	}
	tr.root = remove(tr.root, t.node)
	t.node = nil
}

func (tr *Tree) Len() int {
	if tr.root == nil {
		return 0
	}
	return tr.root.size
}

func (tr *Tree) Min() *Task {
	n := tr.root
	if n == nil {
		return nil
	}
	for n.left != nil {
		n = n.left
	}
	return n.t
}

func (tr *Tree) MinSlice() uint64 {
	if tr.root == nil {
		return 0
	}
	return tr.root.minSlice
}

func (tr *Tree) PickEligible(v uint64) *Task {
	eligible := func(runtime uint64) bool { return !TimeBefore(v, runtime) }
	// In-order traversal is deadline order. If the left subtree has an eligible
	// task, its answer wins; otherwise check this node, then the right subtree.
	for n := tr.root; n != nil; {
		if n.left != nil && eligible(n.left.minV) {
			n = n.left
			continue
		}
		if eligible(n.t.Vruntime) {
			return n.t
		}
		n = n.right
	}
	return nil
}

func (tr *Tree) firstN(limit int, ok func(*Task) bool) *Task {
	if limit <= 0 {
		return nil
	}
	var visit func(*node) *Task
	visit = func(n *node) *Task {
		if n == nil || limit == 0 {
			return nil
		}
		if t := visit(n.left); t != nil {
			return t
		}
		if limit == 0 {
			return nil
		}
		limit--
		if ok(n.t) {
			return n.t
		}
		return visit(n.right)
	}
	return visit(tr.root)
}
