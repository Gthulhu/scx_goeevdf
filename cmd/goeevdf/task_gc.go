package main

import (
	"container/list"
	"errors"
	"fmt"
	"syscall"
	"time"

	"github.com/Gthulhu/scx_goeevdf/internal/bpf"
)

type trackedTask struct {
	ev bpf.Ev
}

// taskGC is owned by one ring consumer. Synthetic exit events must follow the
// same policy/coordinator route as real exits, including slot retirement.
type taskGC struct {
	tasks     map[int32]*list.Element
	order     list.List
	next      uint64
	drops     uint64
	remaining int
}

func (g *taskGC) observe(ev bpf.Ev) {
	if ev.Pid <= 0 {
		return
	}
	if ev.Kind == bpf.EvExit {
		if entry := g.tasks[ev.Pid]; entry != nil {
			g.order.Remove(entry)
			delete(g.tasks, ev.Pid)
		}
		return
	}
	entry := g.tasks[ev.Pid]
	if entry == nil {
		if ev.Kind != bpf.EvEnqueue {
			return
		}
		if g.tasks == nil {
			g.tasks = make(map[int32]*list.Element)
		}
		entry = g.order.PushBack(trackedTask{})
		g.tasks[ev.Pid] = entry
	}
	entry.Value = trackedTask{ev: ev}
}

func (g *taskGC) collect(now, drops uint64, alive func(int32) error, emit func(bpf.Ev) error) error {
	if drops != g.drops {
		g.drops = drops
		g.next = min(g.next, now)
	}
	if g.remaining == 0 && now < g.next {
		return nil
	}
	interval := uint64(time.Second)
	if drops != 0 {
		interval = uint64(100 * time.Millisecond)
	}
	if g.remaining == 0 {
		g.remaining = g.order.Len()
	}
	deadline := time.Now().Add(200 * time.Microsecond)
	for n := 0; n < 64 && g.remaining > 0 && g.order.Len() > 0; n++ {
		if time.Now().After(deadline) {
			break
		}
		g.remaining--
		entry := g.order.Front()
		t := entry.Value.(trackedTask)
		g.order.MoveToBack(entry)
		if now < t.ev.Now || now-t.ev.Now < uint64(2*time.Second) {
			continue
		}
		err := alive(t.ev.Pid)
		if err == nil || errors.Is(err, syscall.EPERM) {
			continue
		}
		if !errors.Is(err, syscall.ESRCH) {
			return fmt.Errorf("check PID %d during task GC: %w", t.ev.Pid, err)
		}
		ev := t.ev
		ev.Kind, ev.Now = bpf.EvExit, now
		if err := emit(ev); err != nil {
			return err
		}
		g.order.Remove(entry)
		delete(g.tasks, ev.Pid)
	}
	if g.order.Len() == 0 {
		g.remaining = 0
	}
	if g.remaining == 0 {
		g.next = now + interval
	}
	return nil
}
