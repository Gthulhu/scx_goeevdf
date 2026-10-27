package shard

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"

	"github.com/Gthulhu/scx_goeevdf/eevdf"
)

// Image is an independent, immutable copy of a task's policy state. Export
// must remove the task from all source queues/packs before returning; Import
// must settle its lag into the destination pack before making it runnable.
// Neither side may retain and mutate the other's image.
type Image []byte

// Values are published at the end of each owner's step. Remote readers see
// one complete (possibly stale) snapshot, never a mixture of CPU fields.
type Values struct {
	Load, Util, W, Queued, BalanceCap, WakeLoad uint64
}

// Engine is exclusively called under its shard's step lock, including from
// concurrent Run/Step callers. Implementations must not call Coordinator
// methods synchronously from these callbacks. In particular, Export and
// Import must implement real policy task removal and adoption; the current
// eevdf.Policy has no such hooks, and cannot be passed directly as an Engine.
// Tick runs the owner's scheduling/periodic work, never remote CPU work.
type Engine interface {
	Handle(eevdf.Event) error
	Export(pid, slot int32) (Image, error)
	Import(pid, slot, cpu int32, image Image) error
	Steal(from int32, level int) (pid, slot int32, ok bool, err error)
	StopFor(cpu, pid, dst int32) error
	Tick() error
	Snapshot(cpu int32) Values
}

type msgKind uint8

const (
	eventMsg msgKind = iota
	transferMsg
	adoptMsg
	restoreMsg
	stealMsg
	stopMsg
)

type message struct {
	kind          msgKind
	event         eevdf.Event
	pid, slot     int32
	dst, from     int32
	level         int
	source        int
	image         Image
}

type ownership struct {
	pid     int32
	cpu     int32
	shard   int
	pending bool
	waiting []message
}

type worker struct {
	mu     sync.Mutex
	engine Engine
	queue  []message
	wake   chan struct{}
}

// Coordinator owns slot registrations and MPSC mailboxes. Slot identity is
// checked against PID, so a reused slot cannot silently deliver old events.
// A slot in transit has no mutable policy owner: events wait until Import (or
// rollback Import) succeeds. No coordinator mutex is held in an Engine call.
type Coordinator struct {
	topology Topology
	mu       sync.Mutex
	slots    map[int32]*ownership
	workers  []worker
	snaps    []atomic.Pointer[Values]
}

// New requires exactly one distinct engine per shard. The caller must create
// each engine with only its shard's CPUs and provide separate per-shard BPF
// event/command rings before starting concurrent Run loops.
func New(t Topology, engines []Engine) (*Coordinator, error) {
	if len(t.CPUs) == 0 || len(engines) != len(t.CPUs) {
		return nil, errors.New("one engine is required per shard")
	}
	copyTopology := Topology{owners: append([]int(nil), t.owners...), CPUs: make([][]int32, len(t.CPUs))}
	for id := range t.CPUs {
		copyTopology.CPUs[id] = append([]int32(nil), t.CPUs[id]...)
	}
	c := &Coordinator{topology: copyTopology, slots: make(map[int32]*ownership),
		workers: make([]worker, len(engines)), snaps: make([]atomic.Pointer[Values], len(t.owners))}
	for id, engine := range engines {
		if engine == nil {
			return nil, fmt.Errorf("nil engine for shard %d", id)
		}
		c.workers[id].engine = engine
		c.workers[id].wake = make(chan struct{}, 1)
	}
	return c, nil
}

func (c *Coordinator) signal(id int) {
	select {
	case c.workers[id].wake <- struct{}{}:
	default:
	}
}

// appendLocked is called with c.mu held. The unbounded mailbox avoids a full
// channel deadlocking a shard while it is trying to hand off a task.
func (c *Coordinator) appendLocked(id int, m message) {
	c.workers[id].queue = append(c.workers[id].queue, m)
	c.signal(id)
}

// Route accepts an event from any ring. A new task must first arrive as an
// enqueue; subsequent events follow its slot owner, not the event CPU. Idle
// events have no task identity. Caller supplies the BPF task slot explicitly.
func (c *Coordinator) Route(slot int32, ev eevdf.Event) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if ev.Kind == eevdf.EvIdle {
		id, ok := c.topology.OwnerCPU(ev.Cpu)
		if !ok {
			return fmt.Errorf("invalid idle CPU %d", ev.Cpu)
		}
		c.appendLocked(id, message{kind: eventMsg, event: ev})
		return nil
	}
	if slot < 0 || ev.Pid <= 0 {
		return errors.New("task event requires nonnegative slot and positive PID")
	}
	o := c.slots[slot]
	if o == nil {
		if ev.Kind != eevdf.EvEnqueue {
			return fmt.Errorf("unknown slot %d", slot)
		}
		id, ok := c.topology.OwnerCPU(ev.Cpu)
		if !ok {
			return fmt.Errorf("invalid task CPU %d", ev.Cpu)
		}
		o = &ownership{pid: ev.Pid, cpu: ev.Cpu, shard: id}
		c.slots[slot] = o
	}
	if o.pid != ev.Pid {
		return fmt.Errorf("slot %d belongs to PID %d, not %d", slot, o.pid, ev.Pid)
	}
	m := message{kind: eventMsg, event: ev, pid: ev.Pid, slot: slot}
	if o.pending {
		o.waiting = append(o.waiting, m)
	} else {
		c.appendLocked(o.shard, m)
	}
	return nil
}

// Handoff requests ownership transfer. The source processes Export first;
// only then can the destination Import. The destination CPU must be remote.
func (c *Coordinator) Handoff(pid, slot, dst int32) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	id, ok := c.topology.OwnerCPU(dst)
	if !ok {
		return fmt.Errorf("invalid destination CPU %d", dst)
	}
	o := c.slots[slot]
	if o == nil || o.pid != pid || o.pending {
		return fmt.Errorf("PID %d slot %d has no transferable owner", pid, slot)
	}
	if id == o.shard {
		return errors.New("handoff destination is local")
	}
	c.appendLocked(o.shard, message{kind: transferMsg, pid: pid, slot: slot, dst: dst})
	return nil
}

// Steal asks the source owner to select an eligible local task. The source
// chooses via Engine.Steal and then performs the ordinary handoff protocol.
func (c *Coordinator) Steal(from, dst int32, level int) error {
	src, ok := c.topology.OwnerCPU(from)
	if !ok {
		return fmt.Errorf("invalid source CPU %d", from)
	}
	dest, ok := c.topology.OwnerCPU(dst)
	if !ok || dest == src {
		return fmt.Errorf("destination CPU %d must be in another shard", dst)
	}
	c.mu.Lock()
	c.appendLocked(src, message{kind: stealMsg, from: from, dst: dst, level: level})
	c.mu.Unlock()
	return nil
}

// StopFor delivers an active-balance stop request only to the source CPU's
// owner; its eventual enqueue is routed by slot as usual.
func (c *Coordinator) StopFor(cpu, pid, dst int32) error {
	src, ok := c.topology.OwnerCPU(cpu)
	if !ok {
		return fmt.Errorf("invalid source CPU %d", cpu)
	}
	dest, ok := c.topology.OwnerCPU(dst)
	if !ok || dest == src {
		return fmt.Errorf("destination CPU %d must be in another shard", dst)
	}
	c.mu.Lock()
	c.appendLocked(src, message{kind: stopMsg, pid: pid, from: cpu, dst: dst})
	c.mu.Unlock()
	return nil
}

// Owner returns false while a slot is unregistered or in transit.
func (c *Coordinator) Owner(pid, slot int32) (int, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	o := c.slots[slot]
	if o == nil || o.pid != pid || o.pending {
		return 0, false
	}
	return o.shard, true
}

func (c *Coordinator) Snapshot(cpu int32) (Values, bool) {
	if _, ok := c.topology.OwnerCPU(cpu); !ok {
		return Values{}, false
	}
	p := c.snaps[cpu].Load()
	if p == nil {
		return Values{}, false
	}
	return *p, true
}

func (c *Coordinator) flushLocked(o *ownership) {
	for _, m := range o.waiting {
		c.appendLocked(o.shard, m)
	}
	o.waiting = nil
}

func (c *Coordinator) process(id int, m message) error {
	e := c.workers[id].engine
	switch m.kind {
	case eventMsg:
		if m.event.Kind != eevdf.EvIdle {
			c.mu.Lock()
			o := c.slots[m.slot]
			if o == nil || o.pid != m.pid {
				c.mu.Unlock()
				return nil // stale event after exit/reuse
			}
			if o.pending {
				o.waiting = append(o.waiting, m)
				c.mu.Unlock()
				return nil
			}
			if o.shard != id {
				c.appendLocked(o.shard, m)
				c.mu.Unlock()
				return nil
			}
			c.mu.Unlock()
		}
		if err := e.Handle(m.event); err != nil {
			return err
		}
		if m.event.Kind == eevdf.EvExit {
			c.mu.Lock()
			if o := c.slots[m.slot]; o != nil && o.pid == m.pid && o.shard == id && !o.pending {
				delete(c.slots, m.slot)
			}
			c.mu.Unlock()
		}
	case transferMsg:
		dest, _ := c.topology.OwnerCPU(m.dst)
		c.mu.Lock()
		o := c.slots[m.slot]
		if o == nil || o.pid != m.pid || o.shard != id || o.pending {
			c.mu.Unlock()
			return nil // stale or duplicate request
		}
		o.pending = true
		c.mu.Unlock()
		image, err := e.Export(m.pid, m.slot)
		if err != nil {
			c.mu.Lock()
			o.pending = false // Export must leave task untouched on error
			c.flushLocked(o)
			c.mu.Unlock()
			return err
		}
		c.mu.Lock()
		c.appendLocked(dest, message{kind: adoptMsg, source: id, pid: m.pid, slot: m.slot, dst: m.dst, from: o.cpu, image: append(Image(nil), image...)})
		c.mu.Unlock()
	case adoptMsg, restoreMsg:
		err := e.Import(m.pid, m.slot, m.dst, append(Image(nil), m.image...))
		if err != nil {
			if m.kind == adoptMsg {
				c.mu.Lock()
				c.appendLocked(m.source, message{kind: restoreMsg, source: m.source, pid: m.pid, slot: m.slot, dst: m.from, image: m.image})
				c.mu.Unlock()
			} else {
				// Leave the slot in transit; retrying Import cannot race an
				// event handler. The caller must resolve persistent failures.
				c.mu.Lock()
				c.appendLocked(id, m)
				c.mu.Unlock()
			}
			return err
		}
		c.mu.Lock()
		o := c.slots[m.slot]
		o.shard, o.cpu, o.pending = id, m.dst, false
		c.flushLocked(o)
		c.mu.Unlock()
	case stealMsg:
		pid, slot, ok, err := e.Steal(m.from, m.level)
		if err != nil || !ok {
			return err
		}
		c.mu.Lock()
		o := c.slots[slot]
		if o != nil && o.pid == pid && o.shard == id && !o.pending {
			c.appendLocked(id, message{kind: transferMsg, pid: pid, slot: slot, dst: m.dst})
		}
		c.mu.Unlock()
	case stopMsg:
		return e.StopFor(m.from, m.pid, m.dst)
	}
	return nil
}

// Step drains at most 256 messages for one shard, invokes its scheduling
// tick, and atomically publishes each owned CPU's snapshot. A single goroutine
// stepping shard IDs in order gives deterministic transport; Run uses the same
// operation with one goroutine per shard.
func (c *Coordinator) Step(id int) (int, error) {
	if id < 0 || id >= len(c.workers) {
		return 0, fmt.Errorf("invalid shard %d", id)
	}
	w := &c.workers[id]
	w.mu.Lock()
	defer w.mu.Unlock()
	n := 0
	for n < 256 {
		c.mu.Lock()
		if len(w.queue) == 0 {
			c.mu.Unlock()
			break
		}
		m := w.queue[0]
		w.queue[0] = message{}
		w.queue = w.queue[1:]
		c.mu.Unlock()
		n++
		if err := c.process(id, m); err != nil {
			return n, err
		}
	}
	if err := w.engine.Tick(); err != nil {
		return n, err
	}
	for _, cpu := range c.topology.CPUs[id] {
		v := w.engine.Snapshot(cpu)
		c.snaps[cpu].Store(&v)
	}
	return n, nil
}

// Run executes one shard's event loop. Caller starts one Run per shard,
// provides ring-reader goroutines calling Route, and handles returned errors.
func (c *Coordinator) Run(ctx context.Context, id int) error {
	if id < 0 || id >= len(c.workers) {
		return fmt.Errorf("invalid shard %d", id)
	}
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-c.workers[id].wake:
			if _, err := c.Step(id); err != nil {
				return err
			}
			c.mu.Lock()
			more := len(c.workers[id].queue) != 0
			c.mu.Unlock()
			if more {
				c.signal(id)
			}
		}
	}
}
