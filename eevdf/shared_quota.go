package eevdf

import (
	"fmt"
	"sync"
)

// SharedQuotaPool owns the remaining cpu.max budget for cgroups used by more
// than one Policy. Each Policy still owns its CPU-local slices and backlog.
// Bind policies before configuring quotas or starting their owner goroutines.
type SharedQuotaPool struct {
	mu     sync.Mutex
	groups map[uint64]*sharedQuota
}

type sharedQuota struct {
	quota, period, remaining, nextRefill, generation uint64
}

func NewSharedQuotaPool() *SharedQuotaPool {
	return &SharedQuotaPool{groups: make(map[uint64]*sharedQuota)}
}

func (s *SharedQuotaPool) Bind(p *Policy) error {
	if s == nil || p == nil || len(p.bandwidth) != 0 {
		return fmt.Errorf("bind a policy before configuring cpu.max")
	}
	if p.sharedQuota != nil {
		return fmt.Errorf("policy already bound to a quota pool")
	}
	p.sharedQuota = s
	return nil
}

// Configure applies a changed cpu.max definition once, before each shard
// refreshes its local group. Identical configurations do not restart a period.
// A zero quota disables the limit for all bound policies immediately.
func (s *SharedQuotaPool) Configure(id, quota, period uint64) error {
	if s == nil || id == 0 || (quota != 0 && period == 0) {
		return fmt.Errorf("invalid shared cpu.max configuration")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.configureLocked(id, quota, period)
	return nil
}

func (s *SharedQuotaPool) configureLocked(id, quota, period uint64) *sharedQuota {
	q := s.groups[id]
	if q == nil {
		q = &sharedQuota{}
		s.groups[id] = q
	}
	if quota == 0 {
		period = 0
	}
	if q.quota != quota || q.period != period {
		q.quota, q.period, q.remaining, q.nextRefill = quota, period, quota, 0
		q.generation++
	}
	return q
}

// Refill must be called by exactly one refiller (normally shard 0). Other
// shard owners call their own Policy.Periodic to release local backlogs.
func (s *SharedQuotaPool) Refill(now uint64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, q := range s.groups {
		if q.quota == 0 {
			continue
		}
		if q.nextRefill == 0 {
			q.nextRefill = now + q.period
		} else if !TimeBefore(now, q.nextRefill) {
			periods := (now-q.nextRefill)/q.period + 1
			q.nextRefill += periods * q.period
			q.remaining = q.quota
			q.generation++
		}
	}
}

// syncLocal is called under the shared pool lock, by the policy's owner.
// Lazily invalidating old-period reservations prevents another shard from
// carrying stale CPU-local budget into the new period.
func (b *bwGroup) syncLocal() {
	if b.generation != b.shared.generation {
		clear(b.local)
		clear(b.exhausted)
		b.generation = b.shared.generation
	}
}
