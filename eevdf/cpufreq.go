package eevdf

// PerfTarget calculates a CPU's performance target in scheduler units.
// Utilization and capacity use the same scale (normally 0..1024). A queued
// task in addition to the current task means the CPU has saturated demand.
// The minimum nonzero target matters because the BPF tick ignores target 0.
func PerfTarget(utilization, capacity, runnable uint64) uint64 {
	if runnable > 1 {
		return Nice0Weight
	}
	if capacity == 0 {
		capacity = Nice0Weight
	}
	// Inputs outside the scheduler's scale must not overflow the calculation.
	capacity = min(capacity, Nice0Weight)
	utilization = min(utilization, capacity)
	return max(1, min(utilization*1280/capacity, Nice0Weight))
}

type frequencyCPU struct {
	target, updatedAt uint64
	valid             bool
}

// FrequencyGovernor keeps independent, rate-limited targets for each CPU.
// It contains no IO; the caller owns the governor and publishes changed
// targets to the per-CPU BPF perf_target control table.
type FrequencyGovernor struct {
	cpus        []frequencyCPU
	rateLimitNs uint64
}

func NewFrequencyGovernor(cpus int, rateLimitNs uint64) *FrequencyGovernor {
	return &FrequencyGovernor{
		cpus:        make([]frequencyCPU, cpus),
		rateLimitNs: rateLimitNs,
	}
}

// Update returns the current target and whether it changed and needs to be
// published. Call on the 1 ms heartbeat and with attach=true immediately
// after placing new work. Attach raises the target immediately when runnable
// work exists, even before utilization catches up; other changes are limited
// to one per rateLimitNs. A backward clock jump rebases the rate limit.
func (g *FrequencyGovernor) Update(cpu int32, now, utilization, capacity, runnable uint64, attach bool) (target uint64, changed bool) {
	if cpu < 0 || int(cpu) >= len(g.cpus) {
		return 0, false
	}
	st := &g.cpus[cpu]
	next := PerfTarget(utilization, capacity, runnable)
	if attach && runnable > 0 {
		next = Nice0Weight
	}
	if st.valid && st.target == next {
		return st.target, false
	}
	if st.valid && now >= st.updatedAt && now-st.updatedAt < g.rateLimitNs &&
		!(attach && next > st.target) {
		return st.target, false
	}
	st.target, st.updatedAt, st.valid = next, now, true
	return next, true
}

// Target returns the last published target, or zero if the CPU is invalid or
// has not yet been sampled.
func (g *FrequencyGovernor) Target(cpu int32) uint64 {
	if cpu < 0 || int(cpu) >= len(g.cpus) {
		return 0
	}
	return g.cpus[cpu].target
}
