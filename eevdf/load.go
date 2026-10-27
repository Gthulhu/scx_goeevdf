package eevdf

import "math"

const half = float64(UtilHalfLifeNs)

type Ravg struct {
	val, at float64
	cur     float64
}

func (r *Ravg) accumulate(input float64, now uint64, halfLife float64) {
	dt := float64(now) - r.at
	if dt > 0 {
		decay := math.Exp2(-dt / halfLife)
		r.val = r.val*decay + r.cur*(1-decay)
		r.at = float64(now)
	}
	r.cur = input
}

func (r *Ravg) read(now uint64, halfLife float64) float64 {
	dt := float64(now) - r.at
	if dt <= 0 {
		return r.val
	}
	decay := math.Exp2(-dt / halfLife)
	return r.val*decay + r.cur*(1-decay)
}

type taskLoadState struct {
	run Ravg
	utilEst, runnableEst, wokeAt, sleptAt uint64
}

type cpuLoad struct {
	run, load Ravg
	wakeLoad, idleStamp uint64
}

type Load struct {
	p *Policy
	cpus []cpuLoad
	tasks map[*Task]*taskLoadState
}

func (l *Load) state(t *Task) *taskLoadState {
	s := l.tasks[t]
	if s == nil {
		s = new(taskLoadState)
		l.tasks[t] = s
	}
	return s
}

func (l *Load) taskRunning(t *Task, running bool, now uint64) {
	input := float64(0)
	if running { input = 1 }
	l.state(t).run.accumulate(input, now, half)
}

func (l *Load) taskWoke(t *Task, now uint64) {
	s := l.state(t)
	if s.wokeAt != 0 && s.sleptAt != 0 {
		s.runnableEst = runnableUpdate(s.runnableEst, satSub(now, s.wokeAt), satSub(s.sleptAt, s.wokeAt))
	}
	s.wokeAt = now
}

func (l *Load) taskQuiesce(t *Task, sleep bool, now uint64) {
	s := l.state(t)
	// Record utilization before switching the input to zero.
	util := l.taskUtilNow(t, now)
	l.taskRunning(t, false, now)
	if sleep {
		s.sleptAt = now
		s.utilEst = utilEstUpdate(s.utilEst, util)
	}
}

func (l *Load) taskUtilNow(t *Task, now uint64) uint64 {
	return min(uint64(l.state(t).run.read(now, half)*1024), 1024)
}
func (l *Load) taskUtil(t *Task, now uint64) uint64 {
	return max(l.taskUtilNow(t, now), l.state(t).utilEst)
}
func (l *Load) taskLoad(t *Task, now uint64) uint64 {
	s := l.state(t)
	asleep := uint64(0)
	if t.state == Sleeping { asleep = satSub(now, s.sleptAt) }
	return taskLoad(t.Weight, l.taskUtil(t, now), s.runnableEst, asleep)
}
func (l *Load) cpuRunning(c int32, running bool, now uint64) {
	input := float64(0)
	if running { input = 1 }
	l.cpus[c].run.accumulate(input, now, half)
}
func (l *Load) cpuUtil(c int32, now uint64) uint64 {
	return min(uint64(l.cpus[c].run.read(now, half)*1024), 1024)
}
func (l *Load) cpuLoad(c int32, now uint64) uint64 {
	return uint64(l.cpus[c].load.read(now, half))
}
func (l *Load) cpuWakeLoad(c int32) uint64 { return l.cpus[c].wakeLoad }
func (l *Load) sample(now uint64) {
	for c := range l.cpus {
		l.cpus[c].load.accumulate(float64(l.p.packs[c].W), now, half)
		l.cpus[c].wakeLoad = l.cpuLoad(int32(c), now)
	}
}
