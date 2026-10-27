package eevdf

const activeMaxIntervalMs = 512

const (
	imbNone = iota
	imbCapacity
	imbRemotePack
	imbLocalSMT
	imbLocalPack
)

type actCPU struct {
	reserved bool
	reservedAt, interval, next uint64
}
type Active struct {
	p *Policy
	cpus []actCPU
	migrate map[*Task]int32
}

func (a *Active) vacant(cpu int32) bool {
	return a.p.io.Idle(cpu) && a.p.cpus[cpu].task == nil &&
		a.p.trees[cpu].Len() == 0 && !a.cpus[cpu].reserved
}
func (a *Active) bestDestination(t *Task, src int32, now uint64) (int32, int) {
	p := a.p
	best := int32(-1)
	for _, dst := range t.Allowed {
		if !p.valid(dst) || dst == src || !a.vacant(dst) { continue }
		if p.topo.Capacity[dst] > p.topo.Capacity[src] &&
			p.ld.taskUtil(t, now)*1280 >= p.topo.Capacity[src]*1024 {
			if best < 0 || p.topo.Capacity[dst] > p.topo.Capacity[best] { best = dst }
		}
	}
	if best >= 0 { return best, imbCapacity }
	if p.topo.SMT {
		busySibling := false
		for c := range p.cpus {
			cpu := int32(c)
			if cpu != src && p.topo.sameCore(cpu, src) &&
				(p.cpus[cpu].task != nil || !p.io.Idle(cpu)) {
				busySibling = true
				break
			}
		}
		if busySibling {
			for _, dst := range t.Allowed {
				if !p.valid(dst) || !p.topo.sameLLC(dst, src) ||
					p.topo.sameCore(dst, src) || !a.vacant(dst) { continue }
				whole := true
				for c := range p.cpus {
					cpu := int32(c)
					if cpu != dst && p.topo.sameCore(cpu, dst) && !a.vacant(cpu) {
						whole = false
						break
					}
				}
				if whole { return dst, imbLocalSMT }
			}
		}
	}
	return -1, imbNone
}

// request stops a running donor. The follow-up non-wakeup enqueue performs
// the transfer; the destination reservation prevents duplicate stop requests.
func (a *Active) request(t *Task, src, dst int32, now uint64) bool {
	st := &a.cpus[dst]
	if st.reserved || now < st.next { return false }
	if _, pending := a.migrate[t]; pending { return false }
	if err := a.p.io.Submit(Cmd{Kind: CmdStop, Pid: t.Pid, Cpu: src}); err != nil {
		a.miss(dst, now)
		return false
	}
	st.reserved, st.reservedAt = true, now
	a.migrate[t] = dst
	return true
}

func (a *Active) run(now uint64) {
	// Expire stale requests first so they can be retried after backoff.
	for t, dst := range a.migrate {
		if now >= a.cpus[dst].reservedAt && now-a.cpus[dst].reservedAt > a.p.TickNs {
			delete(a.migrate, t)
			a.cpus[dst].reserved = false
			a.miss(dst, now)
		}
	}
	for c := range a.p.cpus {
		src := int32(c)
		t := a.p.cpus[c].task
		if t == nil || len(t.Allowed) < 2 { continue }
		dst, kind := a.bestDestination(t, src, now)
		if kind != imbNone { a.request(t, src, dst, now) }
	}
}

func (a *Active) handedOver(t *Task, now uint64) (int32, bool) {
	dst, ok := a.migrate[t]
	if !ok { return 0, false }
	delete(a.migrate, t)
	a.cpus[dst].reserved = false
	a.cpus[dst].interval = 0
	a.cpus[dst].next = now
	return dst, true
}
func (a *Active) miss(dst int32, now uint64) {
	st := &a.cpus[dst]
	st.interval = min(max(st.interval*2, 1), activeMaxIntervalMs)
	st.next = now + st.interval*1_000_000
}

func (a *Active) newidleRequest(dst int32, now uint64) {
	if a.cpus[dst].reserved || !a.vacant(dst) { return }
	var donor *Task
	var source int32
	var heaviest uint64
	for c := range a.p.cpus {
		src := int32(c)
		t := a.p.cpus[c].task
		if t == nil || src == dst || !a.p.allowed(t, dst) || a.p.trees[src].Len() != 0 { continue }
		// Prefer a busy source in the closest topology domain.
		load := a.p.ld.taskLoad(t, now)
		if a.p.topo.sameLLC(src, dst) { load += Nice0Weight }
		if donor == nil || load > heaviest {
			donor, source, heaviest = t, src, load
		}
	}
	if donor != nil { a.request(donor, source, dst, now) }
}
