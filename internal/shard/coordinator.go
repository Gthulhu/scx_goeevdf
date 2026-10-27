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
// methods synchronously from these callbacks. PolicyEngine adapts a real
// eevdf.Policy with local CPU mapping and queued-task export/import; a raw
// eevdf.Policy cannot be passed directly as an Engine. Tick runs the owner's
// scheduling work, never remote CPU work.
type Engine interface {
	Handle(slot int32, event eevdf.Event) error
	Export(pid, slot int32) (Image, error)
	Import(pid, slot, cpu int32, image Image) error
	Steal(from, dst int32, level int) (pid, slot int32, ok bool, err error)
	StopFor(cpu, pid, dst int32) error
	Tick() error
	Snapshot(cpu int32) Values
}

type msgKind uint8

var ErrActiveStopUnsupported = errors.New("cross-shard active STOP requires acknowledged kernel stop")

const (
	eventMsg msgKind = iota
	transferMsg
	adoptMsg
	restoreMsg
	stealMsg
	stopMsg
	periodicMsg
	configMsg
)

type message struct {
	kind      msgKind
	event     eevdf.Event
	pid, slot int32
	dst, from int32
	level     int
	source    int
	image     Image
	now       uint64
	config    Config
}

type ownership struct {
	pid         int32
	slot        int32
	cpu         int32
	shard       int
	pending     bool
	deferRemote bool
	deferCPU    int32
	waiting     []message
}

type worker struct {
	mu      sync.Mutex
	engine  Engine
	queue   []message
	wake    chan struct{}
	running bool
}

// Coordinator owns slot registrations and MPSC mailboxes. Slot identity is
// checked against PID, so a reused slot cannot silently deliver old events.
// A slot in transit has no mutable policy owner: events wait until Import (or
// rollback Import) succeeds. No coordinator mutex is held in an Engine call.
type Coordinator struct {
	topology Topology
	mu       sync.Mutex
	slots    map[int32]*ownership
	byPID    map[int32]*ownership
	nextSlot int32
	free     []int32
	workers  []worker
	snaps    []atomic.Pointer[Values]
}

// New requires exactly one distinct engine per shard. The caller must create
// each engine with only its shard's CPUs and provide separate per-shard BPF
// event/command rings before starting concurrent Run loops.
func New(t Topology, engines []Engine) (*Coordinator, error) {
	if len(t.CPUs) == 0 || len(t.owners) == 0 || len(engines) != len(t.CPUs) {
		return nil, errors.New("one engine is required per shard")
	}
	copyTopology := Topology{owners: append([]int(nil), t.owners...), CPUs: make([][]int32, len(t.CPUs))}
	for id := range t.CPUs {
		copyTopology.CPUs[id] = append([]int32(nil), t.CPUs[id]...)
	}
	c := &Coordinator{topology: copyTopology, slots: make(map[int32]*ownership),
		byPID: make(map[int32]*ownership), nextSlot: 1,
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

// slotLocked allocates a stable global publication slot for a BPF task whose
// first enqueue carries wire slot zero. The first dispatch sends this slot to
// BPF; subsequent kernel events must report the same nonzero slot.
func (c *Coordinator) slotLocked() (int32, error) {
	for len(c.free) != 0 {
		last := len(c.free) - 1
		slot := c.free[last]
		c.free = c.free[:last]
		if c.slots[slot] == nil {
			return slot, nil
		}
	}
	for c.nextSlot < int32(eevdf.MaxTaskSlots) {
		slot := c.nextSlot
		c.nextSlot++
		if c.slots[slot] == nil {
			return slot, nil
		}
	}
	return 0, errors.New("no free task publication slots")
}

// Shards and CPUs allow the caller to create per-shard ring readers, IO
// writers, and scheduling threads without exposing mutable topology storage.
func (c *Coordinator) Shards() int { return len(c.workers) }

func (c *Coordinator) CPUs(id int) []int32 {
	if id < 0 || id >= len(c.workers) {
		return nil
	}
	return append([]int32(nil), c.topology.CPUs[id]...)
}

func (c *Coordinator) OwnerCPU(cpu int32) (int, bool) {
	return c.topology.OwnerCPU(cpu)
}

// Wake requests a periodic Tick even when no events are pending. It is
// coalesced with other wakeups; a timer may call it for each running shard.
func (c *Coordinator) Wake(id int) error {
	if id < 0 || id >= len(c.workers) {
		return fmt.Errorf("invalid shard %d", id)
	}
	c.signal(id)
	return nil
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

// Route accepts an event from any ring. Before its first dispatch BPF reports
// slot zero; the coordinator registers that task by PID and allocates a unique
// publication slot passed to its Policy. Subsequent nonzero BPF slots must
// match the allocated slot. Idle events have no task identity.
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
	if slot < 0 || slot >= int32(eevdf.MaxTaskSlots) || ev.Pid <= 0 {
		return errors.New("task event requires valid slot and positive PID")
	}
	o := c.byPID[ev.Pid]
	if o != nil && slot != 0 && o.slot != slot {
		return fmt.Errorf("PID %d has publication slot %d, not BPF slot %d", ev.Pid, o.slot, slot)
	}
	if o == nil {
		if ev.Kind != eevdf.EvEnqueue {
			return fmt.Errorf("unknown PID %d slot %d", ev.Pid, slot)
		}
		id, ok := c.topology.OwnerCPU(ev.Cpu)
		if !ok {
			return fmt.Errorf("invalid task CPU %d", ev.Cpu)
		}
		if slot == 0 {
			var err error
			slot, err = c.slotLocked()
			if err != nil {
				return err
			}
		}
		if previous := c.slots[slot]; previous != nil {
			return fmt.Errorf("slot %d belongs to PID %d", slot, previous.pid)
		}
		o = &ownership{pid: ev.Pid, slot: slot, cpu: ev.Cpu, shard: id}
		c.slots[slot] = o
		c.byPID[ev.Pid] = o
	}
	slot = o.slot
	if ev.Kind == eevdf.EvEnqueue {
		if _, ok := c.topology.OwnerCPU(ev.Cpu); !ok {
			return fmt.Errorf("invalid task CPU %d", ev.Cpu)
		}
	}
	m := message{kind: eventMsg, event: ev, pid: ev.Pid, slot: slot}
	if o.pending {
		o.waiting = append(o.waiting, m)
	} else {
		c.appendLocked(o.shard, m)
	}
	return nil
}

// Slot returns the canonical publication slot, including for a task whose
// kernel event still reports zero before its first dispatch.
func (c *Coordinator) Slot(pid int32) (int32, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	o := c.byPID[pid]
	if o == nil {
		return 0, false
	}
	return o.slot, true
}

func (c *Coordinator) ownerLocked(pid, wireSlot int32) *ownership {
	o := c.byPID[pid]
	if o == nil || (wireSlot != 0 && o.slot != wireSlot) {
		return nil
	}
	return o
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
	o := c.ownerLocked(pid, slot)
	if o == nil || o.pending {
		return fmt.Errorf("PID %d slot %d has no transferable owner", pid, slot)
	}
	if id == o.shard {
		return errors.New("handoff destination is local")
	}
	c.appendLocked(o.shard, message{kind: transferMsg, pid: pid, slot: o.slot, dst: dst})
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
	if support, ok := c.workers[src].engine.(interface{ SupportsStopFor() bool }); ok && !support.SupportsStopFor() {
		return ErrActiveStopUnsupported
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
	o := c.ownerLocked(pid, slot)
	if o == nil || o.pending {
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
			if m.event.Kind == eevdf.EvEnqueue {
				if dest, ok := c.topology.OwnerCPU(m.event.Cpu); ok && dest != id {
					deferred := o.deferRemote && o.deferCPU == m.event.Cpu
					if deferred {
						o.deferRemote = false
					}
					if transfer, ok := e.(interface {
						HandleForTransfer(int32, eevdf.Event, int32) error
					}); ok {
						sourceCPU := o.cpu
						o.pending = true
						c.mu.Unlock()
						if err := transfer.HandleForTransfer(m.slot, m.event, sourceCPU); err != nil {
							c.mu.Lock()
							o.pending = false
							c.flushLocked(o)
							c.mu.Unlock()
							return err
						}
						image, err := e.Export(m.pid, m.slot)
						c.mu.Lock()
						if err != nil {
							o.pending = false
							c.flushLocked(o)
							c.mu.Unlock()
							if errors.Is(err, eevdf.ErrTaskNotTransferable) {
								return nil // source retained task; retry only on a later event
							}
							return err
						}
						c.appendLocked(dest, message{kind: adoptMsg, source: id,
							pid: m.pid, slot: m.slot, dst: m.event.Cpu, from: sourceCPU,
							image: append(Image(nil), image...)})
						c.mu.Unlock()
						return nil
					}
					if deferred {
						sourceCPU := o.cpu
						c.mu.Unlock()
						event := m.event
						event.Cpu = sourceCPU
						if err := e.Handle(m.slot, event); err != nil {
							return err
						}
						c.mu.Lock()
						c.appendLocked(id, message{kind: transferMsg, pid: m.pid, slot: m.slot, dst: m.event.Cpu})
						c.mu.Unlock()
						return nil
					}
					// Preserve per-slot event order: put Export and this
					// enqueue ahead of any later events already in the
					// mailbox. The enqueue waits while the task is in transit.
					w := &c.workers[id]
					w.queue = append([]message{{kind: transferMsg, pid: m.pid, slot: m.slot, dst: m.event.Cpu}, m}, w.queue...)
					c.mu.Unlock()
					return nil
				}
			}
			c.mu.Unlock()
		}
		if m.event.Kind == eevdf.EvCpumask {
			if a, ok := e.(interface {
				AffinityTarget(int32) (int32, bool, error)
			}); ok {
				dst, migrate, err := a.AffinityTarget(m.pid)
				if err != nil {
					return err
				}
				if migrate {
					if ready, ok := e.(interface{ CanTransfer(int32, int32) bool }); ok &&
						!ready.CanTransfer(m.pid, m.slot) {
						migrate = false // process locally until a later enqueue
					}
				}
				if migrate {
					target, valid := c.topology.OwnerCPU(dst)
					if !valid || target == id {
						return fmt.Errorf("invalid affinity destination CPU %d", dst)
					}
					c.mu.Lock()
					w := &c.workers[id]
					w.queue = append([]message{{kind: transferMsg, pid: m.pid, slot: m.slot, dst: dst}, m}, w.queue...)
					c.mu.Unlock()
					return nil
				}
			}
		}
		if err := e.Handle(m.slot, m.event); err != nil {
			return err
		}
		if m.event.Kind == eevdf.EvEnqueue {
			c.mu.Lock()
			if o := c.slots[m.slot]; o != nil && o.pid == m.pid && o.shard == id {
				if dest, ok := c.topology.OwnerCPU(m.event.Cpu); ok && dest == id {
					o.cpu = m.event.Cpu
				}
			}
			c.mu.Unlock()
		}
		if m.event.Kind == eevdf.EvExit {
			c.mu.Lock()
			if o := c.slots[m.slot]; o != nil && o.pid == m.pid && o.shard == id && !o.pending {
				delete(c.slots, m.slot)
				delete(c.byPID, m.pid)
				c.free = append(c.free, m.slot)
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
			if errors.Is(err, eevdf.ErrTaskNotTransferable) {
				o.deferRemote, o.deferCPU = true, m.dst
			}
			c.flushLocked(o)
			c.mu.Unlock()
			if errors.Is(err, eevdf.ErrTaskNotTransferable) {
				return nil // running/parked/blocked is ordinary, not corruption
			}
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
		o.shard, o.cpu, o.pending, o.deferRemote = id, m.dst, false, false
		c.flushLocked(o)
		c.mu.Unlock()
	case stealMsg:
		pid, slot, ok, err := e.Steal(m.from, m.dst, m.level)
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
	case periodicMsg:
		if p, ok := e.(interface{ Periodic(uint64) }); ok {
			p.Periodic(m.now)
		}
	case configMsg:
		return e.(interface{ Configure(Config) error }).Configure(m.config)
	}
	return nil
}

// Step drains at most 256 messages for one shard, invokes its scheduling
// tick, and atomically publishes each owned CPU's snapshot. A single goroutine
// stepping shard IDs in order gives deterministic transport; Run uses the same
// operation with one goroutine per shard.
func (c *Coordinator) step(id int, fromRun bool) (int, error) {
	if id < 0 || id >= len(c.workers) {
		return 0, fmt.Errorf("invalid shard %d", id)
	}
	w := &c.workers[id]
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.running && !fromRun {
		return 0, fmt.Errorf("shard %d is owned by Run; external Step is forbidden", id)
	}
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

// Step is for deterministic simulation. It cannot run on a shard owned by
// Run, even though its policy lock would serialize access.
func (c *Coordinator) Step(id int) (int, error) { return c.step(id, false) }

// Periodic queues a monotonic-clock heartbeat for the shard's owner loop;
// no Policy method is invoked on the caller's goroutine. Run or the next
// deterministic Step performs the periodic refill/balance before Schedule.
func (c *Coordinator) Periodic(id int, now uint64) error {
	if id < 0 || id >= len(c.workers) {
		return fmt.Errorf("invalid shard %d", id)
	}
	c.mu.Lock()
	c.appendLocked(id, message{kind: periodicMsg, now: now})
	c.mu.Unlock()
	return nil
}

// Run executes one shard's event loop. Caller starts one Run per shard,
// provides ring-reader goroutines calling Route, and handles returned errors.
func (c *Coordinator) Run(ctx context.Context, id int) error {
	if id < 0 || id >= len(c.workers) {
		return fmt.Errorf("invalid shard %d", id)
	}
	w := &c.workers[id]
	w.mu.Lock()
	if w.running {
		w.mu.Unlock()
		return fmt.Errorf("shard %d already has a Run owner", id)
	}
	w.running = true
	w.mu.Unlock()
	defer func() {
		w.mu.Lock()
		w.running = false
		w.mu.Unlock()
	}()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-c.workers[id].wake:
			if _, err := c.step(id, true); err != nil {
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
