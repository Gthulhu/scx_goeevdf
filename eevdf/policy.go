package eevdf

// Policy implements fair.c's enqueue_entity, pick_eevdf, and update_curr.
// One goroutine must own Handle, Schedule, Periodic and cgroup configuration.

const (
	EvEnqueue uint32 = 1 + iota
	EvSleep
	EvDequeue
	EvExit
	EvKeep
	EvIdle
	EvYield
	EvCpumask
)

const (
	CmdDispatch uint32 = 1
	CmdKick     uint32 = 2
	SchedBatch  uint32 = 3
	SchedIdle   uint32 = 5
)

type Event struct {
	Kind, Policy, WakeFlags uint32
	Pid, Tgid, Cpu          int32
	WakerPid, WakerCpu      int32
	Flags, Now, SumExec     uint64
	Weight, CustomSlice     uint64
	EnqSeq                  uint64
	Cgid                    uint64
}

type Cmd struct {
	Kind, Cflags, Slot                 uint32
	Pid, Cpu, ExpectCurr               int32
	Slice, EnqFlags, EnqSeq, KickFlags uint64
}

type IO interface {
	Submit(Cmd) error
	Idle(cpu int32) bool
	CurrPid(cpu int32) int32
	SetQueued(cpu int32, n, keepSlice uint64)
}

type Enums struct {
	EnqWakeup, EnqPreempt uint64
}

type cpuState struct {
	task    *Task
	protect uint64
}

type Policy struct {
	Slice, TickNs uint64
	Enum          Enums
	io            IO
	packs         []*Pack
	trees         []Tree
	cpus          []cpuState
	tasks         map[int32]*Task
	handoff       []int32
	seq           uint64
	groups        map[uint64]*Group
	bandwidth     []*bwGroup
}

func New(io IO, cpus int, enums Enums) *Policy {
	p := &Policy{
		Slice: BaseSlice(cpus), TickNs: 1_000_000, Enum: enums, io: io,
		packs: make([]*Pack, cpus), trees: make([]Tree, cpus),
		cpus: make([]cpuState, cpus), tasks: make(map[int32]*Task),
		groups: make(map[uint64]*Group),
	}
	for c := range p.packs {
		p.packs[c] = &Pack{CPU: int32(c)}
		p.syncQueued(int32(c))
	}
	return p
}

func (p *Policy) valid(cpu int32) bool { return cpu >= 0 && int(cpu) < len(p.cpus) }

func (p *Policy) syncQueued(c int32) {
	p.io.SetQueued(c, uint64(p.trees[c].Len()), p.Slice)
}

func (p *Policy) removeQueued(t *Task) {
	if t.state == Queued {
		p.trees[t.Cpu].Remove(t)
		p.syncQueued(t.Cpu)
	}
}

func (p *Policy) charge(t *Task, sum uint64) {
	if sum < t.lastSum {
		t.lastSum = sum
		return
	}
	delta := sum - t.lastSum
	t.lastSum = sum
	if delta == 0 {
		return
	}
	exhausted := p.chargeBandwidth(t, delta)
	old := t.Vruntime
	t.Vruntime = advance(old, delta, t.Weight)
	if t.pack != nil {
		t.pack.fold(t, t.Vruntime-old)
	}
	if exhausted {
		p.parkThrottled()
	}
}

func (p *Policy) request(t *Task) uint64 {
	if t.CustomSlice != 0 {
		return t.CustomSlice
	}
	return p.Slice
}

func (p *Policy) prepare(t *Task) {
	t.Req.Prepare(t.Vruntime, p.request(t), t.Weight, t.initial)
	t.initial = false
}

func (p *Policy) Handle(e Event) {
	switch e.Kind {
	case EvEnqueue:
		p.enqueue(e)
	case EvSleep, EvDequeue, EvExit:
		p.leave(e)
	case EvKeep:
		if t := p.tasks[e.Pid]; t != nil {
			p.charge(t, e.SumExec)
			p.prepare(t)
		}
	case EvYield:
		p.yield(e)
	case EvCpumask:
		if t := p.tasks[e.Pid]; t != nil {
			p.cpumask(t)
		}
	case EvIdle:
		if p.valid(e.Cpu) {
			p.handoff = append(p.handoff, e.Cpu)
		}
	}
}

func (p *Policy) cpumask(t *Task) {
	p.refreshAllowed(t)
	if p.allowed(t, t.Cpu) {
		return
	}
	var dst int32
	found := false
	for _, cpu := range t.Allowed {
		if p.valid(cpu) {
			dst, found = cpu, true
			break
		}
	}
	if !found {
		return // Affinity is unavailable or has no CPU this policy can use.
	}
	if t.state == Parked {
		t.Cpu = dst
		p.unpark()
		return
	}
	if t.state != Queued {
		return // The next enqueue will relocate a running or sleeping task.
	}
	from := t.Cpu
	p.removeQueued(t)
	t.state = Sleeping
	p.packs[dst].place(t, false)
	t.Cpu = dst
	p.recomputeWeights(from)
	p.recomputeWeights(dst)
	p.prepare(t)
	if p.throttled(t) {
		p.park(t)
		return
	}
	p.queue(t, dst, false)
}

func (p *Policy) enqueue(e Event) {
	if !p.valid(e.Cpu) {
		return
	}
	t := p.tasks[e.Pid]
	if t == nil {
		t = &Task{Pid: e.Pid, Tgid: e.Tgid, Cpu: e.Cpu, initial: true}
		p.tasks[e.Pid] = t
	}
	p.removeQueued(t)
	p.removeBacklog(t)
	p.charge(t, e.SumExec)
	t.state = Sleeping
	t.EnqSeq, t.Policy, t.CustomSlice = e.EnqSeq, e.Policy, e.CustomSlice
	p.refreshAllowed(t)
	nice := LoadWeight(e.Weight)
	if t.Weight == 0 {
		t.Weight = nice
	}
	t.Nice, t.Cgid = nice, e.Cgid
	wake := e.Flags&p.Enum.EnqWakeup != 0
	from := t.Cpu
	wasCurr := p.valid(from) && p.cpus[from].task == t
	if wasCurr {
		p.cpus[from].task = nil
	}
	if wake && t.pack != nil {
		t.pack.quiesce(t, true, max(p.request(t), p.Slice)+p.TickNs)
	}
	dst := p.selectCPU(t, e, wake)
	p.packs[dst].place(t, wake || t.initial)
	if from != dst && p.valid(from) {
		p.recomputeWeights(from)
	}
	p.recomputeWeights(dst)
	p.prepare(t)
	if p.throttled(t) {
		p.park(t)
		if wasCurr {
			p.handoff = append(p.handoff, from)
		}
		return
	}
	if wasCurr && !wake && from == dst && p.keepRunning(t, from) {
		p.dispatch(t, from, false)
		return
	}
	p.queue(t, dst, wake)
	if wasCurr {
		p.handoff = append(p.handoff, from)
	}
}

func (p *Policy) queue(t *Task, cpu int32, wake bool) {
	t.Cpu, t.state = cpu, Queued
	p.seq++
	t.seq = p.seq
	p.trees[cpu].Insert(t)
	p.syncQueued(cpu)
	if p.cpus[cpu].task == nil && p.trees[cpu].Len() == 1 && p.io.Idle(cpu) {
		p.dispatch(t, cpu, false)
	} else if wake && p.shouldPreempt(t, cpu) {
		p.dispatch(t, cpu, true)
	}
}

func (p *Policy) sliceFor(t *Task) uint64 {
	d := int64(t.Req.Deadline - t.Vruntime)
	if d <= 0 {
		return 10_000
	}
	return max(uint64(d)*max(t.Weight, MinDLWeight)/Nice0Weight, 10_000)
}

func (p *Policy) dispatch(t *Task, cpu int32, preempt bool) {
	if p.throttled(t) {
		p.park(t)
		return
	}
	p.removeQueued(t)
	cmd := Cmd{Kind: CmdDispatch, Pid: t.Pid, Cpu: cpu, EnqSeq: t.EnqSeq, Slice: p.sliceFor(t)}
	cmd.Slice = p.capSlice(t, cmd.Slice)
	if cmd.Slice == 0 {
		p.park(t)
		return
	}
	if preempt {
		cmd.EnqFlags = p.Enum.EnqPreempt
		cmd.ExpectCurr = p.io.CurrPid(cpu)
	}
	if err := p.io.Submit(cmd); err != nil {
		p.queueRetry(t, cpu)
		return
	}
	p.reserve(t, cmd.Slice)
	t.Cpu, t.state = cpu, Dispatched
	p.cpus[cpu].task = t
	p.cpus[cpu].protect = t.Req.Deadline
}

func (p *Policy) queueRetry(t *Task, cpu int32) {
	t.Cpu, t.state = cpu, Queued
	p.seq++
	t.seq = p.seq
	p.trees[cpu].Insert(t)
	p.syncQueued(cpu)
}

func (p *Policy) pick(cpu int32) *Task {
	pk := p.packs[cpu]
	if t := p.trees[cpu].PickEligible(pk.Ref); t != nil {
		return t
	}
	return p.trees[cpu].Min()
}

func (p *Policy) keepRunning(t *Task, cpu int32) bool {
	if TimeBefore(p.packs[cpu].Ref, t.Vruntime) {
		return false
	}
	head := p.trees[cpu].PickEligible(p.packs[cpu].Ref)
	return head == nil || !TimeBefore(head.Req.Deadline, t.Req.Deadline)
}

func (p *Policy) leave(e Event) {
	t := p.tasks[e.Pid]
	if t == nil {
		return
	}
	p.removeQueued(t)
	p.removeBacklog(t)
	p.charge(t, e.SumExec)
	if t.pack != nil {
		t.pack.quiesce(t, e.Kind == EvSleep, max(p.request(t), p.Slice)+p.TickNs)
		p.recomputeWeights(t.Cpu)
	}
	if e.Kind == EvSleep {
		t.Req.Sleep()
	}
	if p.valid(t.Cpu) && p.cpus[t.Cpu].task == t {
		p.cpus[t.Cpu].task = nil
		p.handoff = append(p.handoff, t.Cpu)
	}
	t.state = Sleeping
	if e.Kind == EvExit {
		delete(p.tasks, t.Pid)
	}
}

func (p *Policy) yield(e Event) {
	t := p.tasks[e.Pid]
	if t == nil || t.pack == nil {
		return
	}
	p.charge(t, e.SumExec)
	if TimeBefore(t.pack.Ref, t.Vruntime) || !TimeBefore(t.Vruntime, t.Req.Deadline) {
		return
	}
	d := t.Req.Deadline - t.Vruntime
	t.Vruntime = t.Req.Deadline
	t.pack.fold(t, d)
	p.prepare(t)
}

func (p *Policy) Schedule() {
	p.unpark()
	for _, c := range p.handoff {
		if p.cpus[c].task == nil {
			if t := p.pick(c); t != nil {
				p.dispatch(t, c, false)
			}
		}
	}
	p.handoff = p.handoff[:0]
	for c := range p.cpus {
		for p.cpus[c].task == nil && p.trees[c].Len() != 0 {
			t := p.pick(int32(c))
			p.dispatch(t, int32(c), false)
			if t.state == Queued { // failed Submit; retry on the next Schedule
				break
			}
		}
		if p.cpus[c].task == nil && p.trees[c].Len() == 0 && p.io.Idle(int32(c)) {
			p.pull(int32(c))
		}
	}
}
