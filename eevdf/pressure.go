package eevdf

import (
	"errors"
	"math"
	"math/bits"
)

// Pressure implements scx_reevdf pressure.rs capacity pressure
// (fair.c's check_cpu_capacity). The scheduling goroutine owns its state.
const (
	pressureEvalNs     uint64 = 32_000_000
	pressureIdleDropNs uint64 = 1_000_000_000
)

// StatSample holds cumulative per-CPU /proc/stat irq, softirq and steal time
// in nanoseconds. It excludes user/system time, which is not lost capacity.
type StatSample struct {
	IRQ, SoftIRQ, Steal uint64
}

type pressureCPU struct {
	at, displacedAt uint64
	statAt          StatSample
	idleAt, avail   uint64
	demand, valid   bool
}

type Pressure struct {
	cpus []pressureCPU
}

func NewPressure(cpus int) *Pressure {
	return &Pressure{cpus: make([]pressureCPU, cpus)}
}

// Update consumes cumulative displaced_ns (cpu_release to cpu_acquire) and
// a cumulative /proc/stat sample. It returns true when a window is evaluated.
// Call on the same scheduling goroutine that owns the policy; no IO is done here.
func (pr *Pressure) Update(cpu int32, now uint64, demand bool, displaced uint64, stat StatSample) bool {
	if cpu < 0 || int(cpu) >= len(pr.cpus) {
		return false
	}
	st := &pr.cpus[cpu]
	if !demand {
		if st.demand {
			st.idleAt = now
		}
		st.demand = false
		if st.valid && now >= st.idleAt && now-st.idleAt > pressureIdleDropNs {
			st.valid = false
		}
		return false
	}
	if !st.demand {
		if st.valid && now >= st.idleAt && now-st.idleAt > pressureIdleDropNs {
			st.valid = false
		}
		st.demand = true
		st.at, st.displacedAt, st.statAt = now, displaced, stat
		return false
	}
	// A clock reset starts a new window; never interpret it as a huge elapsed time.
	if now < st.at {
		st.at, st.displacedAt, st.statAt = now, displaced, stat
		return false
	}
	elapsed := now - st.at
	if elapsed < pressureEvalNs {
		return false
	}
	lost := satAddPressure(satSub(displaced, st.displacedAt), satSub(stat.IRQ, st.statAt.IRQ))
	lost = satAddPressure(lost, satSub(stat.SoftIRQ, st.statAt.SoftIRQ))
	lost = satAddPressure(lost, satSub(stat.Steal, st.statAt.Steal))
	var available uint64
	if lost < elapsed {
		hi, lo := bits.Mul64(lost, Nice0Weight)
		fraction, _ := bits.Div64(hi, lo, elapsed)
		available = Nice0Weight - fraction
	}
	if st.valid {
		st.avail = (3*st.avail + available) / 4
	} else {
		st.avail, st.valid = available, true
	}
	st.at, st.displacedAt, st.statAt = now, displaced, stat
	return true
}

func satAddPressure(a, b uint64) uint64 {
	if b > math.MaxUint64-a {
		return math.MaxUint64
	}
	return a + b
}

// Available returns the smoothed capacity fraction (0..1024). An idle CPU
// or one without a completed demand window has its full capacity available.
func (pr *Pressure) Available(cpu int32) uint64 {
	if cpu < 0 || int(cpu) >= len(pr.cpus) {
		return Nice0Weight
	}
	st := &pr.cpus[cpu]
	if !st.demand || !st.valid {
		return Nice0Weight
	}
	return st.avail
}

// BalanceCapacity scales a CPU's topology capacity for balancing decisions.
func (pr *Pressure) BalanceCapacity(cpu int32, capacity uint64) uint64 {
	avail := pr.Available(cpu)
	hi, lo := bits.Mul64(capacity, avail)
	scaled, _ := bits.Div64(hi, lo, Nice0Weight)
	return max(scaled, 1)
}

var errProcStat = errors.New("invalid /proc/stat CPU line")

// ParseProcStat fills preallocated per-CPU samples from a /proc/stat snapshot.
// hz is USER_HZ (ticks per second, typically 100), not the kernel clock rate.
// The caller can reuse both the input buffer and samples across polls.
// Missing CPUs retain their previous samples; CPU IDs outside samples are ignored.
func ParseProcStat(data []byte, samples []StatSample, hz uint64) error {
	if hz == 0 {
		return errProcStat
	}
	for len(data) > 0 {
		line := data
		if i := byteIndex(data, '\n'); i >= 0 {
			line, data = data[:i], data[i+1:]
		} else {
			data = nil
		}
		if len(line) < 4 || line[0] != 'c' || line[1] != 'p' || line[2] != 'u' ||
			line[3] < '0' || line[3] > '9' {
			continue
		}
		i := 3
		var cpu uint64
		for i < len(line) && line[i] >= '0' && line[i] <= '9' {
			digit := uint64(line[i] - '0')
			if cpu > (math.MaxUint64-digit)/10 {
				return errProcStat
			}
			cpu = cpu*10 + digit
			i++
		}
		if i == len(line) || (line[i] != ' ' && line[i] != '\t') {
			return errProcStat
		}
		var ticks [8]uint64
		for field := range ticks {
			for i < len(line) && (line[i] == ' ' || line[i] == '\t') {
				i++
			}
			if i == len(line) || line[i] < '0' || line[i] > '9' {
				return errProcStat
			}
			for i < len(line) && line[i] >= '0' && line[i] <= '9' {
				digit := uint64(line[i] - '0')
				if ticks[field] > (math.MaxUint64-digit)/10 {
					return errProcStat
				}
				ticks[field] = ticks[field]*10 + digit
				i++
			}
			if i < len(line) && line[i] != ' ' && line[i] != '\t' && line[i] != '\r' {
				return errProcStat
			}
		}
		if cpu < uint64(len(samples)) {
			samples[cpu] = StatSample{
				IRQ: ticksToNs(ticks[5], hz), SoftIRQ: ticksToNs(ticks[6], hz),
				Steal: ticksToNs(ticks[7], hz),
			}
		}
	}
	return nil
}

func byteIndex(data []byte, b byte) int {
	for i, v := range data {
		if v == b {
			return i
		}
	}
	return -1
}

func ticksToNs(ticks, hz uint64) uint64 {
	whole, rem := ticks/hz, ticks%hz
	if whole > math.MaxUint64/1_000_000_000 {
		return math.MaxUint64
	}
	hi, lo := bits.Mul64(rem, 1_000_000_000)
	fraction, _ := bits.Div64(hi, lo, hz)
	return satAddPressure(whole*1_000_000_000, fraction)
}
