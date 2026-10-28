package eevdf

// FrequencySample returns the current CPU demand on the policy goroutine.
// The runnable count includes the current task as well as queued work.
func (p *Policy) FrequencySample(cpu int32, now uint64) (util, capacity, runnable uint64) {
	if !p.valid(cpu) {
		return 0, 0, 0
	}
	util = p.ld.cpuUtil(cpu, now)
	capacity = p.topo.Capacity[cpu]
	runnable = uint64(p.trees[cpu].Len())
	if p.cpus[cpu].task != nil {
		runnable++
	}
	return
}

// TaskCPU reports the policy's selected CPU for a runnable task.
func (p *Policy) TaskCPU(pid int32) (int32, bool) {
	t := p.tasks[pid]
	if t == nil || !p.valid(t.Cpu) || (t.state != Queued && t.state != Dispatched) {
		return 0, false
	}
	return t.Cpu, true
}
