package eevdf

import (
	"sync"
	"sync/atomic"
	"testing"
)

type blockingQuotaIO struct {
	*fakeIO
	entered chan struct{}
	release chan struct{}
}

func (i *blockingQuotaIO) Submit(cmd Cmd) error {
	close(i.entered)
	<-i.release
	return i.fakeIO.Submit(cmd)
}

func sharedQuotaPolicy(t *testing.T, pool *SharedQuotaPool, id, quota uint64) (*Policy, *fakeIO) {
	t.Helper()
	io := &fakeIO{n: make([]uint64, 1)}
	p := New(io, 1, Enums{})
	if err := pool.Bind(p); err != nil {
		t.Fatal(err)
	}
	if err := p.SetCgroup(id, 0, 100); err != nil {
		t.Fatal(err)
	}
	if err := p.SetCPUQuota(id, quota, 1_000_000); err != nil {
		t.Fatal(err)
	}
	return p, io
}

func TestSharedQuotaOnlyOneShardCanBorrowBurst(t *testing.T) {
	pool := NewSharedQuotaPool()
	p1, io1 := sharedQuotaPolicy(t, pool, 7, 100_000)
	p2, io2 := sharedQuotaPolicy(t, pool, 7, 100_000)
	pool.Refill(10_000_000)
	p1.Periodic(10_000_000)
	p2.Periodic(10_000_000)
	p1.Handle(Event{Kind: EvEnqueue, Pid: 1, Cpu: 0, Cgid: 7, Weight: 100})
	p2.Handle(Event{Kind: EvEnqueue, Pid: 2, Cpu: 0, Cgid: 7, Weight: 100})
	if len(io1.cmds) != 1 || io1.cmds[0].Slice != 100_000 ||
		len(io2.cmds) != 0 || p2.tasks[2].state != Parked {
		t.Fatalf("shared quota double-spent: first %+v, second %+v", io1.cmds, io2.cmds)
	}
	pool.Refill(10_999_999)
	p2.Periodic(10_999_999)
	if len(io2.cmds) != 0 {
		t.Fatal("second shard dispatched before refill")
	}
	pool.Refill(11_000_000)
	p2.Periodic(11_000_000)
	if len(io2.cmds) != 1 || io2.cmds[0].Slice != 100_000 {
		t.Fatalf("second shard did not receive new budget: %+v", io2.cmds)
	}
	// A stale local reservation from the previous period must be discarded.
	p1.Periodic(11_000_000)
	p1.Handle(Event{Kind: EvEnqueue, Pid: 3, Cpu: 0, Cgid: 7, Weight: 100})
	if len(io1.cmds) != 1 {
		t.Fatal("old local budget was spent again after another shard borrowed new budget")
	}
}

func TestSharedQuotaReservedBeforeSubmitting(t *testing.T) {
	pool := NewSharedQuotaPool()
	io1 := &blockingQuotaIO{
		fakeIO:  &fakeIO{n: make([]uint64, 1)},
		entered: make(chan struct{}), release: make(chan struct{}),
	}
	p1 := New(io1, 1, Enums{})
	if err := pool.Bind(p1); err != nil {
		t.Fatal(err)
	}
	if err := p1.SetCgroup(1, 0, 100); err != nil {
		t.Fatal(err)
	}
	if err := p1.SetCPUQuota(1, 10_000, 1_000_000); err != nil {
		t.Fatal(err)
	}
	p2, io2 := sharedQuotaPolicy(t, pool, 1, 10_000)
	done := make(chan struct{})
	go func() {
		defer close(done)
		p1.Handle(Event{Kind: EvEnqueue, Pid: 1, Cpu: 0, Cgid: 1, Weight: 100})
	}()
	<-io1.entered
	p2.Handle(Event{Kind: EvEnqueue, Pid: 2, Cpu: 0, Cgid: 1, Weight: 100})
	close(io1.release)
	<-done
	if len(io1.cmds) != 1 || len(io2.cmds) != 0 || p2.tasks[2].state != Parked {
		t.Fatalf("concurrent submit over-allocated shared quota: %+v %+v", io1.cmds, io2.cmds)
	}
}

func TestSharedQuotaConcurrentReservationsAndHierarchy(t *testing.T) {
	pool := NewSharedQuotaPool()
	var policies [4]*Policy
	for i := range policies {
		p, _ := sharedQuotaPolicy(t, pool, 1, 200_000)
		if err := p.SetCgroup(2, 1, 100); err != nil {
			t.Fatal(err)
		}
		if err := p.SetCPUQuota(2, 70_000, 1_000_000); err != nil {
			t.Fatal(err)
		}
		policies[i] = p
	}
	var total atomic.Uint64
	var wg sync.WaitGroup
	for i, p := range policies {
		wg.Add(1)
		go func(i int, p *Policy) {
			defer wg.Done()
			task := &Task{Pid: int32(i + 1), Cpu: 0, Cgid: 2, state: Dispatched}
			for j := 0; j < 100; j++ {
				slice := p.reserveSlice(task, 1_000)
				total.Add(slice)
				p.chargeBandwidth(task, slice)
			}
		}(i, p)
	}
	wg.Wait()
	if got := total.Load(); got != 70_000 {
		t.Fatalf("hierarchical shared limit allocated %d, want 70000", got)
	}
	pool.mu.Lock()
	root, child := pool.groups[1].remaining, pool.groups[2].remaining
	pool.mu.Unlock()
	if root != 130_000 || child != 0 {
		t.Fatalf("ancestor/child remaining %d/%d, want 130000/0", root, child)
	}
}

func TestSharedQuotaFailedDispatchRefundAndReconfigure(t *testing.T) {
	pool := NewSharedQuotaPool()
	p1, io1 := sharedQuotaPolicy(t, pool, 1, 10_000)
	p2, io2 := sharedQuotaPolicy(t, pool, 1, 10_000)
	io1.fail = true
	p1.Handle(Event{Kind: EvEnqueue, Pid: 1, Cpu: 0, Cgid: 1, Weight: 100})
	p2.Handle(Event{Kind: EvEnqueue, Pid: 2, Cpu: 0, Cgid: 1, Weight: 100})
	if len(io2.cmds) != 1 || io2.cmds[0].Slice != 10_000 {
		t.Fatalf("failed submit held shared quota: %+v", io2.cmds)
	}

	if err := p1.SetCPUQuota(1, 20_000, 1_000_000); err == nil {
		t.Fatal("per-policy update must not reconfigure a global quota")
	}

	if err := pool.Configure(1, 20_000, 1_000_000); err != nil {
		t.Fatal(err)
	}
	if err := p1.SetCPUQuota(1, 20_000, 1_000_000); err != nil {
		t.Fatal(err)
	}
	if err := p2.SetCPUQuota(1, 20_000, 1_000_000); err != nil {
		t.Fatal(err)
	}
	if err := p1.SetCPUQuota(1, 0, 0); err == nil {
		t.Fatal("per-policy disable must not bypass shared configuration")
	}
	if err := pool.Configure(1, 0, 0); err != nil {
		t.Fatal(err)
	}
	if err := p1.SetCPUQuota(1, 0, 0); err != nil {
		t.Fatal(err)
	}
	if err := p2.SetCPUQuota(1, 0, 0); err != nil {
		t.Fatal(err)
	}
}

func TestSinglePolicyQuotaRefundsFailedDispatch(t *testing.T) {
	io := &fakeIO{n: make([]uint64, 1), fail: true}
	p := New(io, 1, Enums{})
	if err := p.SetCgroup(1, 0, 100); err != nil {
		t.Fatal(err)
	}

	if err := p.SetCPUQuota(1, 10_000, 1_000_000); err != nil {
		t.Fatal(err)
	}
	p.Handle(Event{Kind: EvEnqueue, Pid: 1, Cpu: 0, Cgid: 1, Weight: 100})
	if p.bandwidth[0].pool != 10_000 || p.bandwidth[0].local[0] != 0 {
		t.Fatal("failed dispatch consumed local-only quota")
	}
	io.fail = false
	p.Schedule()
	if len(io.cmds) != 1 || io.cmds[0].Slice != 10_000 || p.bandwidth[0].pool != 0 {
		t.Fatalf("retry failed to reserve full quota: %+v", io.cmds)
	}
}

func TestSharedQuotaLateRefundCannotCreditNextPeriod(t *testing.T) {
	pool := NewSharedQuotaPool()
	p1, _ := sharedQuotaPolicy(t, pool, 1, 100)
	p2, _ := sharedQuotaPolicy(t, pool, 1, 100)
	task1 := &Task{Cpu: 0, Cgid: 1}
	task2 := &Task{Cpu: 0, Cgid: 1}
	if got := p1.reserveSlice(task1, 100); got != 100 {
		t.Fatalf("first period reserved %d", got)
	}
	pool.Refill(10_000_000)
	pool.Refill(11_000_000)
	if got := p2.reserveSlice(task2, 100); got != 100 {
		t.Fatalf("next period reserved %d", got)
	}
	p1.refundSlice(task1, 100)
	if got := p1.reserveSlice(task1, 100); got != 0 {
		t.Fatalf("old period's late refund created %d new quota", got)
	}
}

func TestSharedQuotaFailedPreemptPreservesEarlierReservation(t *testing.T) {
	pool := NewSharedQuotaPool()
	p, _ := sharedQuotaPolicy(t, pool, 1, 30_000)
	running := &Task{Cpu: 0, Cgid: 1}
	preempting := &Task{Cpu: 0, Cgid: 1}
	if got := p.reserveSlice(running, 10_000); got != 10_000 {
		t.Fatalf("first dispatch reserved %d", got)
	}
	if got := p.reserveSlice(preempting, 20_000); got != 20_000 {
		t.Fatalf("preemptive dispatch reserved %d", got)
	}
	p.refundSlice(preempting, 20_000)
	pool.mu.Lock()
	remaining := pool.groups[1].remaining
	pool.mu.Unlock()
	if remaining != 20_000 || p.bandwidth[0].local[0] != 10_000 {
		t.Fatalf("failed preemption changed prior reservation: pool %d local %d",
			remaining, p.bandwidth[0].local[0])
	}
}
