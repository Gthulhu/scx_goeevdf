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

type wakeState struct {
	flips, decayAt uint64
	lastWakee      int32
}

func (w *wakeState) recordWakee(wakee int32, now uint64) {
	if TimeBefore(w.decayAt+1_000_000_000, now) {
		w.flips >>= 1
		w.decayAt = now
	}
	if w.lastWakee != wakee {
		w.lastWakee = wakee
		w.flips++
	}
}

func (p *Policy) wakeFlag(e Event, flag uint64) bool {
	return flag != 0 && e.WakeFlags&uint32(flag) != 0
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
	if len(t.Allowed) == 0 {
		return prev
	}
	if t.Blocked {
		return prev
	}
	// A delayed negative-lag task remains on its original pack while it owes
	// service; moving it would incorrectly discard its virtual-time debt.
	if d := t.delay; p.F.DelayRequeue && d.pack != nil &&
		d.pack.emptyGen == d.gen && p.allowed(t, d.pack.CPU) {
		if delayDebt(t.lag, int64(p.refAt(d.pack, p.now)-d.ref), d.sum, t.Weight) < 0 {
			return d.pack.CPU
		}
	}

	fork := p.wakeFlag(e, p.Enum.WakeFork)
	if fork && !t.Blocked {
		anchor := e.WakerCpu
		if !p.valid(anchor) {
			anchor = prev
		}
		if c := p.findIdlestFork(t, anchor); p.valid(c) {
			return c
		}
	}
	if wake && !t.Blocked && p.wakeFlag(e, p.Enum.WakeCurrentCPU) &&
		p.valid(e.WakerCpu) && p.allowed(t, e.WakerCpu) {
		return e.WakerCpu
	}

	target := prev
	if wake && p.wakeFlag(e, p.Enum.WakeTTWU) {
		target = p.wakeAffine(t, e, prev)
	}
	if !wake {
		return target
	}
	if p.vacant(target) {
		return target
	}
	if p.valid(e.WakerCpu) && p.allowed(t, e.WakerCpu) &&
		p.topo.sameLLC(target, e.WakerCpu) && p.vacant(e.WakerCpu) {
		return e.WakerCpu
	}
	if provider, ok := p.io.(CapacityIO); ok && p.F.SISUtil && p.F.CapacityPressure {
		best, capacity := target, uint64(0)
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
	for _, scope := range p.topo.newidleSpans(target) {
		for _, c := range scope {
			if p.allowed(t, c) && p.vacant(c) {
				return c
			}
		}
	}
	return target
}

func (p *Policy) wakeAffine(t *Task, e Event, prev int32) int32 {
	this := e.WakerCpu
	if t.Blocked || !p.valid(this) || !p.wakeFlag(e, p.Enum.WakeTTWU) {
		return prev
	}
	waker := p.tasks[e.WakerPid]
	if waker != nil {
		waker.wake.recordWakee(t.Pid, e.Now)
	}
	if !p.allowed(t, this) || !p.topo.sameNode(prev, this) {
		return prev
	}
	if waker != nil && wakeWide(waker.wake.flips, t.wake.flips,
		uint64(p.llcSize(this))) {
		return prev
	}
	if p.io.Idle(this) {
		if p.io.Idle(prev) {
			return prev
		}
		return this
	}
	sync := p.F.WakeSync && p.wakeFlag(e, p.Enum.WakeSync) && waker != nil
	if sync && p.trees[this].Len() == 0 {
		return this
	}
	if !p.F.WaWeight || p.io.Idle(prev) {
		return prev
	}
	wakerLoad := int64(-1)
	if sync {
		wakerLoad = int64(p.ld.taskLoad(waker, e.Now))
	}
	thisLoad := max(p.ld.cpuWakeLoad(this), p.packs[this].W)
	prevLoad := max(p.ld.cpuWakeLoad(prev), p.packs[prev].W)
	sameCore := p.topo.SMT && p.topo.sameCore(prev, this)
	if wakeAffineWeight(thisLoad, prevLoad, wakerLoad,
		p.ld.taskLoad(t, e.Now), sameCore) {
		return this
	}
	return prev
}

func (p *Policy) llcSize(cpu int32) int {
	size := 0
	for c := range p.topo.LLC {
		if p.topo.sameLLC(cpu, int32(c)) {
			size++
		}
	}
	return size
}

func (p *Policy) findIdlestFork(t *Task, anchor int32) int32 {
	if len(t.Allowed) == 0 {
		return -1
	}
	restricted := len(t.Allowed) < len(p.cpus)
	var best ForkGroup
	haveBest := false
	for _, first := range t.Allowed {
		if !p.valid(first) {
			continue
		}
		seen := false
		for _, prior := range t.Allowed {
			if prior == first {
				break
			}
			if p.valid(prior) && p.topo.sameLLC(prior, first) {
				seen = true
				break
			}
		}
		if seen {
			continue
		}
		g := ForkGroup{Base: uint32(first), Cap: 0,
			Local: p.valid(anchor) && p.topo.sameLLC(first, anchor)}
		for _, cpu := range t.Allowed {
			if !p.valid(cpu) || !p.topo.sameLLC(cpu, first) {
				continue
			}
			g.Allowed++
			g.Nr++
			g.Cap += p.topo.Capacity[cpu]
			g.Load += max(p.ld.cpuWakeLoad(cpu), p.packs[cpu].W)
			g.Util += p.ld.cpuUtil(cpu, p.now)
			if p.vacant(cpu) {
				g.Idle++
			}
			if p.cpus[cpu].task != nil || p.trees[cpu].Len() != 0 {
				g.Runnable++
			}
		}
		if !haveBest || forkGroupWins(g, &best, restricted, true) {
			best, haveBest = g, true
		}
	}
	if !haveBest {
		return -1
	}

	chosen := int32(-1)
	chosenIdle := false
	chosenLoad, chosenCap := uint64(0), uint64(1)
	util := p.ld.taskUtil(t, p.now)
	for _, cpu := range t.Allowed {
		if !p.valid(cpu) || !p.topo.sameLLC(cpu, int32(best.Base)) {
			continue
		}
		idle := p.vacant(cpu)
		load, cap := max(p.ld.cpuWakeLoad(cpu), p.packs[cpu].W), p.topo.Capacity[cpu]
		fits := util*1280 < cap*1024
		chosenFits := util*1280 < chosenCap*1024
		betterCapacity := p.topo.AsymCapacity && idle && chosenIdle &&
			(fits && !chosenFits || fits == chosenFits && fits && cap < chosenCap ||
				!fits && !chosenFits && cap > chosenCap)
		if chosen < 0 || idle && !chosenIdle ||
			idle == chosenIdle && (betterCapacity ||
				!betterCapacity && (!p.topo.AsymCapacity || !idle || fits == chosenFits) &&
					load*chosenCap < chosenLoad*cap) {
			chosen, chosenIdle = cpu, idle
			chosenLoad, chosenCap = load, cap
		}
	}
	return chosen
}
