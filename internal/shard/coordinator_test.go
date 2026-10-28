package shard

import (
	"context"
	"encoding/binary"
	"fmt"
	"math/rand"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/Gthulhu/scx_goeevdf/eevdf"
)

type fakeEngine struct {
	id         int
	tasks      map[int32]int32
	handled    []int32
	importFail bool
	exportBusy bool
	periodic   []uint64
	ticks      chan uint64
}

type blockingStopTarget struct {
	*fakeEngine
	entered chan struct{}
	release chan struct{}
}

func (e *blockingStopTarget) StopTarget(int32, int32, uint64) (int32, bool, bool) {
	close(e.entered)
	<-e.release
	return 0, false, false
}

func TestStopTargetDoesNotHoldCoordinatorLock(t *testing.T) {
	c, engines := fixture(t, 4)
	stop := &blockingStopTarget{fakeEngine: engines[0],
		entered: make(chan struct{}), release: make(chan struct{})}
	c.workers[0].engine = stop
	if err := c.Route(8, eevdf.Event{Kind: eevdf.EvEnqueue, Pid: 8, Cpu: 0}); err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { _, err := c.Step(0); done <- err }()
	select {
	case <-stop.entered:
	case <-time.After(5 * time.Second):
		close(stop.release)
		t.Fatal("StopTarget was not invoked")
	}
	route := make(chan error, 1)
	go func() {
		if _, ok := c.Owner(8, 8); !ok {
			route <- fmt.Errorf("lost owner during StopTarget")
			return
		}
		route <- c.Route(8, eevdf.Event{Kind: eevdf.EvYield, Pid: 8, Cpu: 0})
	}()
	select {
	case err := <-route:
		if err != nil {
			close(stop.release)
			t.Fatal(err)
		}
	case <-time.After(5 * time.Second):
		close(stop.release)
		t.Fatal("StopTarget held coordinator lock across affinity I/O")
	}
	close(stop.release)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(engines[0].handled, []int32{8, 8}) {
		t.Fatalf("later event overtook pending STOP check: %v", engines[0].handled)
	}
}

func (f *fakeEngine) Handle(slot int32, ev eevdf.Event) error {
	f.handled = append(f.handled, ev.Pid)
	if ev.Kind == eevdf.EvExit {
		delete(f.tasks, ev.Pid)
	} else if ev.Kind == eevdf.EvEnqueue {
		f.tasks[ev.Pid] = ev.Cpu
	}
	return nil
}
func (f *fakeEngine) Export(pid, slot int32) (Image, error) {
	if f.exportBusy {
		return nil, eevdf.ErrTaskNotTransferable
	}
	cpu, ok := f.tasks[pid]
	if !ok {
		return nil, fmt.Errorf("source %d lacks task %d", f.id, pid)
	}
	delete(f.tasks, pid)
	var b [4]byte
	binary.LittleEndian.PutUint32(b[:], uint32(cpu))
	return b[:], nil
}
func (f *fakeEngine) Import(pid, slot, cpu int32, image Image) error {
	if f.importFail {
		f.importFail = false
		return fmt.Errorf("import failure")
	}
	if _, ok := f.tasks[pid]; ok || len(image) != 4 {
		return fmt.Errorf("duplicate or invalid image")
	}
	f.tasks[pid] = cpu
	return nil
}
func (f *fakeEngine) Steal(cpu, dst int32, level int) (int32, int32, bool, error) {
	for pid, ownedCPU := range f.tasks {
		if ownedCPU == cpu {
			return pid, pid, true, nil
		}
	}
	return 0, 0, false, nil
}
func (f *fakeEngine) StopFor(cpu, pid, dst int32) error { return nil }
func (f *fakeEngine) Tick() error                       { return nil }
func (f *fakeEngine) Periodic(now uint64) {
	f.periodic = append(f.periodic, now)
	if f.ticks != nil {
		f.ticks <- now
	}
}
func (f *fakeEngine) Snapshot(cpu int32) Values {
	var n uint64
	for _, taskCPU := range f.tasks {
		if taskCPU == cpu {
			n++
		}
	}
	return Values{Queued: n, W: n * 1024, Load: n}
}

func TestNontransferableExportDoesNotSpinOrTerminateShard(t *testing.T) {
	c, engines := fixture(t, 4)
	_ = c.Route(0, eevdf.Event{Kind: eevdf.EvEnqueue, Pid: 55, Cpu: 0})
	stepAll(t, c)
	engines[0].exportBusy = true
	if err := c.Handoff(55, 0, 40); err != nil {
		t.Fatal(err)
	}
	if err := c.Route(0, eevdf.Event{Kind: eevdf.EvEnqueue, Pid: 55, Cpu: 40}); err != nil {
		t.Fatal(err)
	}
	if n, err := c.Step(0); err != nil || n >= 256 {
		t.Fatalf("busy export failed/spun: messages=%d error=%v", n, err)
	}
	if n, err := c.Step(0); err != nil || n >= 256 {
		t.Fatalf("deferred event looped: messages=%d error=%v", n, err)
	}
	if owner, ok := c.Owner(55, 0); !ok || owner != 0 || engines[0].tasks[55] != 0 {
		t.Fatal("busy export lost source ownership or did not handle event locally")
	}
	engines[0].exportBusy = false
	if err := c.Handoff(55, 0, 40); err != nil {
		t.Fatal(err)
	}
	stepAll(t, c)
	stepAll(t, c)
	if owner, ok := c.Owner(55, 0); !ok || owner != 2 {
		t.Fatal("source could not later transfer a pending task")
	}
}

func fixture(t *testing.T, n int) (*Coordinator, []*fakeEngine) {
	t.Helper()
	llcs := make([][]int32, n)
	for id := range llcs {
		for i := 0; i < 20; i++ {
			llcs[id] = append(llcs[id], int32(id*20+i))
		}
	}
	top, err := NewTopology(20*n, llcs, false)
	if err != nil {
		t.Fatal(err)
	}
	engines := make([]Engine, n)
	fakes := make([]*fakeEngine, n)
	for id := range engines {
		fakes[id] = &fakeEngine{id: id, tasks: make(map[int32]int32)}
		engines[id] = fakes[id]
	}
	c, err := New(top, engines)
	if err != nil {
		t.Fatal(err)
	}
	return c, fakes
}

func stepAll(t *testing.T, c *Coordinator) {
	t.Helper()
	for id := range c.workers {
		if _, err := c.Step(id); err != nil {
			t.Fatal(err)
		}
	}
}

func TestTopologyOver64(t *testing.T) {
	llcs := [][]int32{}
	for cpu := int32(0); cpu < 72; cpu += 3 {
		llcs = append(llcs, []int32{cpu, cpu + 1, cpu + 2})
	}
	top, err := NewTopology(72, llcs, false)
	if err != nil {
		t.Fatal(err)
	}
	if len(top.CPUs) != 12 {
		t.Fatalf("got %d shards, want 12", len(top.CPUs))
	}
	for cpu := int32(0); cpu < 72; cpu++ {
		id, _ := top.OwnerCPU(cpu)
		if id != int(cpu/6) {
			t.Fatalf("CPU %d maps to %d, want %d", cpu, id, cpu/6)
		}
	}
	one, err := NewTopology(72, llcs, true)
	if err != nil || len(one.CPUs) != 1 {
		t.Fatalf("forced one: %v, %d", err, len(one.CPUs))
	}
	for _, bad := range [][][]int32{{{0}, {0}}, {{0}, {2}}} {
		if _, err := NewTopology(2, bad, false); err == nil {
			t.Fatal("accepted invalid topology", bad)
		}
	}
}

func TestPeriodicIsDeliveredByShardOwner(t *testing.T) {
	c, engines := fixture(t, 4)
	if err := c.Periodic(0, 123); err != nil {
		t.Fatal(err)
	}
	if len(engines[0].periodic) != 0 {
		t.Fatal("Periodic invoked Policy on its caller's goroutine")
	}
	if _, err := c.Step(0); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(engines[0].periodic, []uint64{123}) {
		t.Fatalf("owner did not deliver exact clock: %v", engines[0].periodic)
	}
	engines[0].ticks = make(chan uint64, 1)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- c.Run(ctx, 0) }()
	defer func() {
		cancel()
		if err := <-done; err != context.Canceled {
			t.Errorf("owner Run returned %v", err)
		}
	}()
	if err := c.Periodic(0, 456); err != nil {
		t.Fatal(err)
	}
	select {
	case now := <-engines[0].ticks:
		if now != 456 {
			t.Fatalf("owner received wrong clock %d", now)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("owner Run did not deliver heartbeat")
	}
	if _, err := c.Step(0); err == nil {
		t.Fatal("external Step entered Policy owned by running goroutine")
	}
	if err := c.Run(ctx, 0); err == nil {
		t.Fatal("second Run shared a Policy owner")
	}
}

func TestZeroWireSlotsHaveDistinctProvisionalOwners(t *testing.T) {
	c, engines := fixture(t, 4)
	for _, ev := range []eevdf.Event{
		{Kind: eevdf.EvEnqueue, Pid: 91, Cpu: 0},
		{Kind: eevdf.EvEnqueue, Pid: 92, Cpu: 20},
	} {
		if err := c.Route(0, ev); err != nil {
			t.Fatal(err)
		}
	}
	first, a := c.Slot(91)
	second, b := c.Slot(92)
	if !a || !b || first == 0 || second == 0 || first == second {
		t.Fatalf("zero-slot tasks conflated: %d %v; %d %v", first, a, second, b)
	}
	stepAll(t, c)
	if id, ok := c.Owner(91, 0); !ok || id != 0 || engines[0].tasks[91] != 0 {
		t.Fatal("first provisional task lost")
	}
	if id, ok := c.Owner(92, 0); !ok || id != 1 || engines[1].tasks[92] != 20 {
		t.Fatal("second provisional task lost")
	}
	if err := c.Route(first, eevdf.Event{Kind: eevdf.EvKeep, Pid: 91, Cpu: 0}); err != nil {
		t.Fatal("BPF publication slot failed to bind to PID:", err)
	}
	if err := c.Route(second, eevdf.Event{Kind: eevdf.EvKeep, Pid: 91, Cpu: 0}); err == nil {
		t.Fatal("task accepted a different PID's BPF slot")
	}
	if err := c.Handoff(91, 0, 40); err != nil {
		t.Fatal("wire slot zero handoff failed:", err)
	}
	stepAll(t, c)
	stepAll(t, c)
	if id, ok := c.Owner(91, first); !ok || id != 2 {
		t.Fatal("provisional slot was not preserved through handoff")
	}
	if err := c.Route(0, eevdf.Event{Kind: eevdf.EvExit, Pid: 92, Cpu: 20}); err != nil {
		t.Fatal(err)
	}
	stepAll(t, c)
	if _, ok := c.Slot(92); ok {
		t.Fatal("exited provisional PID retained its slot")
	}
	if err := c.Route(0, eevdf.Event{Kind: eevdf.EvEnqueue, Pid: 93, Cpu: 20}); err != nil {
		t.Fatal(err)
	}
	if reused, ok := c.Slot(93); !ok || reused != second {
		t.Fatalf("freed publication slot not recycled: %d, want %d", reused, second)
	}
}

func TestConcurrentZeroSlotRegistrations(t *testing.T) {
	c, _ := fixture(t, 4)
	const perShard = 256
	var wg sync.WaitGroup
	errs := make(chan error, 4)
	for shard := 0; shard < 4; shard++ {
		wg.Add(1)
		go func(shard int) {
			defer wg.Done()
			for j := 0; j < perShard; j++ {
				pid := int32(shard*perShard + j + 1)
				if err := c.Route(0, eevdf.Event{Kind: eevdf.EvEnqueue, Pid: pid,
					Cpu: int32(shard * 20)}); err != nil {
					errs <- err
					return
				}
			}
		}(shard)
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Fatal(err)
	}
	seen := make(map[int32]bool)
	for pid := int32(1); pid <= 4*perShard; pid++ {
		slot, ok := c.Slot(pid)
		if !ok || slot <= 0 || seen[slot] {
			t.Fatalf("PID %d has duplicate/missing provisional slot %d", pid, slot)
		}
		seen[slot] = true
	}
}

func TestTransferForwardAndSnapshots(t *testing.T) {
	c, f := fixture(t, 4)
	cpus := c.CPUs(0)
	cpus[0] = 77
	if id, ok := c.OwnerCPU(0); !ok || id != 0 || c.CPUs(0)[0] != 0 {
		t.Fatal("CPU topology was mutable")
	}
	if err := c.Route(7, eevdf.Event{Kind: eevdf.EvEnqueue, Pid: 7, Cpu: 0}); err != nil {
		t.Fatal(err)
	}
	stepAll(t, c)
	if err := c.Handoff(7, 7, 21); err != nil {
		t.Fatal(err)
	}
	if _, err := c.Step(0); err != nil {
		t.Fatal(err)
	}
	if _, ok := c.Owner(7, 7); ok {
		t.Fatal("transit task still reported owned")
	}
	if err := c.Route(7, eevdf.Event{Kind: eevdf.EvKeep, Pid: 7, Cpu: 0}); err != nil {
		t.Fatal(err)
	}
	if len(f[0].tasks) != 0 || len(f[1].tasks) != 0 {
		t.Fatal("task visible during transit")
	}
	stepAll(t, c)
	owner, ok := c.Owner(7, 7)
	if !ok || owner != 1 || len(f[1].handled) != 1 {
		t.Fatalf("adoption/forwarding: owner=%d ok=%v handled=%v", owner, ok, f[1].handled)
	}
	snap, ok := c.Snapshot(21)
	if !ok || snap.Queued != 1 || snap.W != 1024 {
		t.Fatalf("bad snapshot %+v %v", snap, ok)
	}
	if err := c.Route(7, eevdf.Event{Kind: eevdf.EvExit, Pid: 7, Cpu: 0}); err != nil {
		t.Fatal(err)
	}
	stepAll(t, c)
	if _, ok := c.Owner(7, 7); ok {
		t.Fatal("exited slot retained")
	}
	if err := c.Route(7, eevdf.Event{Kind: eevdf.EvEnqueue, Pid: 8, Cpu: 40}); err != nil {
		t.Fatal("reused slot must accept a new PID:", err)
	}
	stepAll(t, c)
	if owner, ok := c.Owner(8, 7); !ok || owner != 2 {
		t.Fatal("slot reuse routed to wrong owner")
	}
}

func TestStealAndMisroutedEvents(t *testing.T) {
	c, f := fixture(t, 4)
	_ = c.Route(11, eevdf.Event{Kind: eevdf.EvEnqueue, Pid: 11, Cpu: 0})
	stepAll(t, c)
	if err := c.Route(11, eevdf.Event{Kind: eevdf.EvYield, Pid: 11, Cpu: 61}); err != nil {
		t.Fatal(err)
	}
	if err := c.Steal(0, 40, 2); err != nil {
		t.Fatal(err)
	}
	stepAll(t, c) // old owner processes event, selects victim and exports
	stepAll(t, c) // new owner imports
	if owner, ok := c.Owner(11, 11); !ok || owner != 2 || len(f[0].handled) != 2 {
		t.Fatalf("steal/misrouting failed: %d %v %v", owner, ok, f[0].handled)
	}
	if err := c.StopFor(0, 11, 41); err == nil {
		t.Fatal("stale source was allowed to stop a task owned by another shard")
	}
	if err := c.Steal(0, 1, 0); err == nil {
		t.Fatal("local steal accepted")
	}
}

func TestRemoteEnqueueTransfersBeforeHandle(t *testing.T) {
	c, engines := fixture(t, 4)
	_ = c.Route(12, eevdf.Event{Kind: eevdf.EvEnqueue, Pid: 12, Cpu: 0})
	stepAll(t, c)
	if err := c.Route(12, eevdf.Event{Kind: eevdf.EvEnqueue, Pid: 12, Cpu: 40}); err != nil {
		t.Fatal(err)
	}
	if _, err := c.Step(0); err != nil {
		t.Fatal(err)
	}
	if len(engines[0].tasks) != 0 || len(engines[0].handled) != 1 {
		t.Fatal("source handled remote enqueue before exporting task")
	}
	stepAll(t, c)
	if id, ok := c.Owner(12, 12); !ok || id != 2 || engines[2].tasks[12] != 40 {
		t.Fatal("remote enqueue not adopted by destination")
	}
}

func TestImportRollback(t *testing.T) {
	c, f := fixture(t, 4)
	_ = c.Route(9, eevdf.Event{Kind: eevdf.EvEnqueue, Pid: 9, Cpu: 0})
	stepAll(t, c)
	f[1].importFail = true
	if err := c.Handoff(9, 9, 20); err != nil {
		t.Fatal(err)
	}
	if _, err := c.Step(0); err != nil {
		t.Fatal(err)
	}
	if _, err := c.Step(1); err == nil {
		t.Fatal("expected import failure")
	}
	if _, ok := c.Owner(9, 9); ok {
		t.Fatal("failed transfer became owned")
	}
	if _, err := c.Step(0); err != nil {
		t.Fatal(err)
	}
	if owner, ok := c.Owner(9, 9); !ok || owner != 0 || f[0].tasks[9] != 0 {
		t.Fatal("failed import did not restore source ownership")
	}
}

func TestDeterministicTransportAndInvariants(t *testing.T) {
	run := func() []string {
		t.Helper()
		c, f := fixture(t, 4)
		trace := []string{}
		for pid := int32(1); pid <= 40; pid++ {
			if err := c.Route(pid, eevdf.Event{Kind: eevdf.EvEnqueue, Pid: pid, Cpu: 0}); err != nil {
				t.Fatal(err)
			}
		}
		stepAll(t, c)
		rng := rand.New(rand.NewSource(42))
		for iteration := 0; iteration < 400; iteration++ {
			pid := int32(rng.Intn(40) + 1)
			owner, ok := c.Owner(pid, pid)
			if !ok {
				t.Fatal("lost task", pid)
			}
			target := (owner + 1 + rng.Intn(3)) % 4
			if err := c.Handoff(pid, pid, int32(target*20)); err != nil {
				t.Fatal(err)
			}
			// A full round, then the destination adoption round.
			stepAll(t, c)
			stepAll(t, c)
			for task := int32(1); task <= 40; task++ {
				id, owned := c.Owner(task, task)
				count := 0
				for shard := range f {
					if _, present := f[shard].tasks[task]; present {
						count++
						if shard != id {
							t.Fatalf("PID %d wrong owner %d != %d", task, shard, id)
						}
					}
				}
				if !owned || count != 1 {
					t.Fatalf("PID %d ownership count=%d, registered=%v", task, count, owned)
				}
			}
			for shard := range f {
				for _, cpu := range c.CPUs(shard) {
					v, published := c.Snapshot(cpu)
					if !published || v.W != v.Queued*1024 || v.Load != v.Queued {
						t.Fatalf("bad snapshot CPU %d: %+v", cpu, v)
					}
				}
			}
			trace = append(trace, fmt.Sprintf("%d:%d", pid, target))
		}
		return trace
	}
	if a, b := run(), run(); !reflect.DeepEqual(a, b) {
		t.Fatal("non-deterministic transport")
	}
}

func TestConcurrentRoutingAndHandoff(t *testing.T) {
	c, f := fixture(t, 4)
	const tasks = 64
	for pid := int32(1); pid <= tasks; pid++ {
		_ = c.Route(pid, eevdf.Event{Kind: eevdf.EvEnqueue, Pid: pid, Cpu: (pid - 1) % 4 * 20})
	}
	stepAll(t, c)
	ctx, cancel := context.WithCancel(context.Background())
	var loops sync.WaitGroup
	errs := make(chan error, 4)
	for id := range f {
		loops.Add(1)
		go func(id int) {
			defer loops.Done()
			if err := c.Run(ctx, id); err != nil && err != context.Canceled {
				errs <- err
			}
		}(id)
	}
	var senders sync.WaitGroup
	for k := 0; k < 4; k++ {
		senders.Add(1)
		go func(k int) {
			defer senders.Done()
			r := rand.New(rand.NewSource(int64(k + 1)))
			for j := 0; j < 25000; j++ {
				pid := int32(k*16 + r.Intn(16) + 1)
				if id, ok := c.Owner(pid, pid); ok {
					dst := (id + 1 + r.Intn(3)) % 4
					_ = c.Handoff(pid, pid, int32(dst*20))
				}
				_ = c.Route(pid, eevdf.Event{Kind: eevdf.EvKeep, Pid: pid, Cpu: 0})
			}
		}(k)
	}
	senders.Wait()
	// Stop the loops, then drain pending transfers in deterministic order.
	cancel()
	loops.Wait()
	close(errs)
	for err := range errs {
		t.Fatal(err)
	}
	for round := 0; round < 2000; round++ {
		stepAll(t, c)
		c.mu.Lock()
		pending := false
		for _, o := range c.slots {
			pending = pending || o.pending
		}
		empty := !pending
		for id := range c.workers {
			empty = empty && len(c.workers[id].queue) == 0
		}
		c.mu.Unlock()
		if empty {
			break
		}
		if round == 1999 {
			t.Fatal("mailboxes did not drain")
		}
	}

	for pid := int32(1); pid <= tasks; pid++ {
		id, ok := c.Owner(pid, pid)
		if !ok {
			t.Fatalf("PID %d stuck in transit", pid)
		}
		count := 0
		for shard := range f {
			if _, found := f[shard].tasks[pid]; found {
				count++
				if shard != id {
					t.Fatalf("PID %d in wrong shard %d", pid, shard)
				}
			}
		}
		if count != 1 {
			t.Fatalf("PID %d in %d shards", pid, count)
		}
	}
}

// One million completed transfers, not merely one million transfer requests.
// The distinct owner loops may also be run concurrently (covered above).
func TestMillionCompletedTransfers(t *testing.T) {
	c, engines := fixture(t, 4)
	const tasks, rounds = 1000, 1000
	for pid := int32(1); pid <= tasks; pid++ {
		if err := c.Route(pid, eevdf.Event{Kind: eevdf.EvEnqueue, Pid: pid, Cpu: 0}); err != nil {
			t.Fatal(err)
		}
	}
	for i := 0; i < 4; i++ {
		if _, err := c.Step(0); err != nil {
			t.Fatal(err)
		}
	}
	src, dst := 0, 1
	for round := 0; round < rounds; round++ {
		for pid := int32(1); pid <= tasks; pid++ {
			if err := c.Handoff(pid, pid, int32(dst*20)); err != nil {
				t.Fatal(err)
			}
		}
		for i := 0; i < 4; i++ {
			if _, err := c.Step(src); err != nil {
				t.Fatal(err)
			}
		}
		for i := 0; i < 4; i++ {
			if _, err := c.Step(dst); err != nil {
				t.Fatal(err)
			}
		}
		if len(engines[src].tasks) != 0 || len(engines[dst].tasks) != tasks {
			t.Fatalf("round %d: duplicate/lost task: source=%d destination=%d", round, len(engines[src].tasks), len(engines[dst].tasks))
		}
		src, dst = dst, src
	}
}
