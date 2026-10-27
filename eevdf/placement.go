package eevdf

// AffinityIO allows the policy to respect the complete Linux CPU affinity mask.
// Without it the BPF-selected CPU is retained rather than guessing an allowed CPU.
type AffinityIO interface {
	Affinity(pid int32) ([]int32, error)
}

type CapacityIO interface {
	Available(cpu int32) uint64
}

func (p *Policy) refreshAllowed(t *Task) {
	if a, ok := p.io.(AffinityIO); ok {
		if cpus, err := a.Affinity(t.Pid); err == nil {
			t.Allowed = cpus
		}
	}
}

func (p *Policy) allowed(t *Task, cpu int32) bool {
	for _, c := range t.Allowed {
		if c == cpu {
			return true
		}
	}
	return false
}

func (p *Policy) vacant(cpu int32) bool {
	return p.valid(cpu) && p.io.Idle(cpu) && p.cpus[cpu].task == nil &&
		p.trees[cpu].Len() == 0 && !p.act.cpus[cpu].reserved
}

func (p *Policy) selectCPU(t *Task, e Event, wake bool) int32 {
	prev := e.Cpu
	if !p.allowed(t, prev) {
		for _, c := range t.Allowed {
			if p.valid(c) {
				prev = c
				break
			}
		}
		if t.Blocked {
			return prev
		}
	}
	if !wake || len(t.Allowed) == 0 {
		return prev
	}
	// A delayed negative-lag task remains on its original pack while it owes
	// service; moving it would incorrectly discard its virtual-time debt.
	if d := t.delay; p.F.DelayRequeue && d.pack != nil &&
		d.pack.emptyGen == d.gen && p.allowed(t, d.pack.CPU) {
		if delayDebt(t.lag, int64(d.pack.Ref-d.ref), d.sum, t.Weight) < 0 {
			return d.pack.CPU
		}
	}
	if p.vacant(prev) {
		return prev
	}
	if p.valid(e.WakerCpu) && p.allowed(t, e.WakerCpu) &&
		p.topo.sameLLC(prev, e.WakerCpu) && p.vacant(e.WakerCpu) {
		return e.WakerCpu
	}
	if provider, ok := p.io.(CapacityIO); ok && p.F.SISUtil && p.F.CapacityPressure {
		best, capacity := prev, uint64(0)
		for _, c := range t.Allowed {
			if p.vacant(c) &&
				provider.Available(c) > capacity {
				best, capacity = c, provider.Available(c)
			}
		}
		if capacity != 0 {
			return best
		}
	}
	for _, scope := range p.topo.newidleSpans(prev) {
		for _, c := range scope {
			if p.allowed(t, c) && p.vacant(c) {
				return c
			}
		}
	}
	return prev
}
