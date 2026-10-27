package eevdf

const (
	Nice0Weight uint64 = 1024
	MinDLWeight uint64 = Nice0Weight / 4
)

func LoadWeight(scx uint64) uint64 { return max(scx*Nice0Weight/100, 1) }

func scaleByDLWeight(request, weight uint64) uint64 {
	return request * Nice0Weight / max(weight, MinDLWeight)
}

func TimeBefore(a, b uint64) bool { return int64(a-b) < 0 }

func vdiv(v int64, d uint64) int64 {
	if d == 0 {
		return 0
	}
	if v < 0 {
		return -int64(uint64(-v) / d)
	}
	return int64(uint64(v) / d)
}

type Request struct{ Deadline, Slice, Weight uint64 }

func (r *Request) Prepare(vtime, slice, weight uint64, initial bool) bool {
	weight = max(weight, MinDLWeight)
	pending := r.Slice != 0 && TimeBefore(vtime, r.Deadline)
	issued := r.Slice != slice || !pending
	if issued {
		req := slice
		if initial {
			req = slice / 2
		}
		r.Deadline = vtime + scaleByDLWeight(req, weight)
	} else if r.Weight != weight {
		r.Deadline = vtime + (r.Deadline-vtime)*r.Weight/weight
	}
	r.Slice, r.Weight = slice, weight
	return issued
}

func (r *Request) Rebase(old, new uint64) {
	if r.Slice != 0 && TimeBefore(old, r.Deadline) {
		r.Deadline = new + (r.Deadline - old)
	} else {
		r.Slice = 0
	}
}

func (r *Request) Sleep() { r.Slice = 0 }

func LagLimit(request, weight uint64) int64 { return int64(scaleByDLWeight(request, weight)) }

func Lag(ref, vtime uint64, limit int64) int64 {
	return min(max(int64(ref-vtime), -limit), limit)
}

func joinDelta(ref, sum, vtime, w uint64) uint64 {
	return uint64(vdiv(int64(w)*int64(vtime-ref), sum+w))
}

func leaveDelta(ref, sum, vtime, w uint64) uint64 {
	if sum <= w {
		return 0
	}
	return uint64(vdiv(int64(w)*int64(ref-vtime), sum-w))
}

func place(ref, sum uint64, lag int64, w uint64) uint64 {
	if sum != 0 {
		lag = vdiv(lag*int64(sum+w), sum)
	}
	return ref - uint64(lag)
}

func fold(w, distance, sum, rem uint64) (advance, rest uint64) {
	if sum == 0 {
		return 0, rem
	}
	acc := w*distance + rem
	advance = acc / sum
	return advance, acc - advance*sum
}

func advance(vtime, delta, w uint64) uint64 {
	return vtime + delta*Nice0Weight/max(w, 1)
}

func delayDebt(lag, adv int64, sum, w uint64) int64 {
	if lag >= 0 {
		return 0
	}
	if adv <= 0 {
		return lag
	}
	return min(lag+vdiv(adv*int64(sum), sum+w), 0)
}

func BaseSlice(ncpu int) uint64 {
	f := uint64(4)
	switch {
	case ncpu <= 1:
		f = 1
	case ncpu <= 3:
		f = 2
	case ncpu <= 7:
		f = 3
	}
	return f * 700_000
}

func referenceAt(ref, sum, w, delta, request uint64) uint64 {
	if w == 0 || sum == 0 || delta >= request {
		return ref
	}
	dist := delta * Nice0Weight / w
	return ref + dist*w/sum
}

func referenceBeforeJoin(ref, sum, w, delta, request, join uint64) uint64 {
	total := sum + join
	if w == 0 || sum == 0 || total == 0 || delta >= request {
		return ref
	}
	proj := (delta * Nice0Weight / w) * w / total
	return ref + proj*total/sum
}

const UtilHalfLifeNs = 32_000_000

func utilEstUpdate(est, dequeued uint64) uint64 {
	if est <= dequeued {
		return dequeued
	}
	return est - (est-dequeued)>>2
}

func runnableUpdate(est, cycle, runnable uint64) uint64 {
	if cycle == 0 {
		return est
	}
	f := uint64(1024)
	if runnable < cycle {
		f = runnable * 1024 / cycle
	}
	return (est*7 + f) / 8
}

func taskLoad(w, utilEst, runnableEst, asleep uint64) uint64 {
	shift := min(asleep/UtilHalfLifeNs, 63)
	util := max(utilEst, runnableEst) >> shift
	return w * min(util, 1024) / 1024
}

func wakeWide(wakerFlips, wakeeFlips, factor uint64) bool {
	master, slave := wakerFlips, wakeeFlips
	if wakerFlips < wakeeFlips {
		master, slave = wakeeFlips, wakerFlips
	}
	return slave >= factor && master >= slave*factor
}

func wakeAffineWeight(thisLoad, prevLoad uint64, wakerLoad int64, tload uint64, sameCore bool) bool {
	thisEff := int64(thisLoad)
	if wakerLoad >= 0 {
		if uint64(wakerLoad) > thisLoad {
			return true
		}
		thisEff -= wakerLoad
	}
	thisEff = (thisEff + int64(tload)) * 100
	pct := int64(100 + (117-100)/2)
	if sameCore {
		pct = 100 + (110-100)/2
	}
	prevEff := (int64(prevLoad) - int64(tload)) * pct
	if wakerLoad >= 0 {
		prevEff++
	}
	return thisEff < prevEff
}

type ForkGroup struct {
	Base, Nr, Idle    uint32
	Load, Util, Cap   uint64
	Recent            uint64
	Runnable, Allowed uint32
	Local             bool
}

func forkGroupWins(g ForkGroup, best *ForkGroup, restricted, coreLevel bool) bool {
	if g.Nr == 0 || g.Allowed == 0 {
		return false
	}
	if best == nil {
		return true
	}
	spare := restricted && g.Runnable < g.Allowed
	bestSpare := restricted && best.Runnable < best.Allowed
	if spare != bestSpare {
		return spare
	}
	if g.Idle != best.Idle {
		return g.Idle > best.Idle
	}
	if spare {
		if best.Local {
			return false
		}
		if g.Local {
			return true
		}
		if g.Util != best.Util {
			return g.Util < best.Util
		}
	} else {
		load, bestLoad := g.Load*best.Cap, best.Load*g.Cap
		if restricted && best.Local != g.Local {
			pct := uint64(125)
			if coreLevel {
				pct = 117
			}
			if best.Local {
				return bestLoad*100 > load*pct
			}
			return load*100 <= bestLoad*pct
		}
		if load != bestLoad {
			return load < bestLoad
		}
		if !restricted && coreLevel && g.Recent != best.Recent {
			return g.Recent < best.Recent
		}
		if best.Local {
			return false
		}
		if g.Local {
			return true
		}
	}
	return coreLevel && g.Recent < best.Recent
}

const NewidleDecayNs = 1_000_000_000

func avgIdleUpdate(avg, idle, maxCost uint64) uint64 {
	if idle > avg {
		avg += (idle - avg) / 8
	} else {
		avg -= (avg - idle) / 8
	}
	return min(avg, 2*maxCost)
}

func newidleCostUpdate(record, stamp, cost, now uint64) (uint64, uint64) {
	switch {
	case cost > record:
		return cost, now
	case TimeBefore(stamp+NewidleDecayNs, now):
		return record * 253 / 256, now
	}
	return record, stamp
}

func newidleStatsFold(calls, succ, stamp, ratio, now uint64) (uint64, uint64, uint64, uint64) {
	if calls < 1024 {
		return calls, succ, stamp, ratio
	}
	var delta uint64
	if !TimeBefore(now, stamp) {
		delta = now - stamp
	}
	return calls / 2, succ / 2, now, min((delta>>22)+succ, 1024)
}

func newidleSample(ratio uint64, random uint32) (weight uint64, ok bool) {
	if ratio >= 1024 {
		return 1, true
	}
	sample := 1 + ratio
	if uint64(random&1023) > sample {
		return 0, false
	}
	return (1024 + sample/2) / sample, true
}

const (
	GroupWeightShift = 10
	GroupMinShares   = 2
)

func cgroupLoadWeight(w uint64) uint64 { return max((w*Nice0Weight+50)/100, 1) }
func propWeight(fixed, shares, load uint64) uint64 {
	return fixed * shares / max(load, 1)
}
func roundWeight(fixed uint64) uint64 {
	return max((fixed+(1<<(GroupWeightShift-1)))>>GroupWeightShift, 1)
}

func groupShares(weight, tasks, cpus, load, loadAvg, tgLoadAvg, contrib uint64) uint64 {
	total := min(max(tasks, 1), max(cpus, 1)) * weight
	load = max(load, loadAvg)
	tgLoad := satSub(tgLoadAvg, contrib) + load
	shares := total
	if tgLoad != 0 {
		shares = total * load / tgLoad
	}
	return min(max(shares, GroupMinShares), total)
}

func satSub(a, b uint64) uint64 {
	if a > b {
		return a - b
	}
	return 0
}
