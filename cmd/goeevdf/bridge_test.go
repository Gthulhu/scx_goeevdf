package main

import (
	"sync/atomic"
	"testing"

	"github.com/Gthulhu/scx_goeevdf/eevdf"
	"github.com/Gthulhu/scx_goeevdf/internal/bpf"
)

func TestPublishTaskSeqcountAndReuse(t *testing.T) {
	b := backend{sched: &bpf.Sched{Task: make([]bpf.TaskPub, 2)}}
	b.Publish(1, eevdf.TaskPub{Pid: 10, CoreVtime: 12, Vruntime: 34,
		Deadline: 56, Lag: -7, Weight: 100})
	pub := &b.sched.Task[1]
	if atomic.LoadUint64(&pub.Seq) != 2 || pub.Pid != 10 ||
		pub.CoreVtime != 12 || pub.Vruntime != 34 || pub.Deadline != 56 ||
		pub.Lag != -7 || pub.Weight != 100 || pub.PublishedAt == 0 {
		t.Fatalf("invalid task publication: %+v", *pub)
	}
	b.Publish(1, eevdf.TaskPub{})
	if atomic.LoadUint64(&pub.Seq) != 4 || pub.Pid != 0 ||
		pub.CoreVtime != 0 || pub.Weight != 0 || pub.PublishedAt != 0 {
		t.Fatalf("slot was not cleared before reuse: %+v", *pub)
	}
	b.Publish(0, eevdf.TaskPub{Pid: 11})
	b.Publish(2, eevdf.TaskPub{Pid: 11})
}
