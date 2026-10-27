package eevdf

// Features contains the fair.c switches. Call Normalize after overriding the
// defaults so dependent switches cannot remain enabled without their parent.
type Features struct {
	WakeupPreempt, Eligibility, EligibleScan, RunToParity, PreemptShort bool
	PlaceLag, PlaceRelDeadline, DelayDequeue, DelayRequeue              bool
	ProxyExec, Hrtick, VrefUpdate, WakeSync, WaWeight, SISUtil          bool
	NewidleCost, NewidleSampling, CapacityPressure                      bool
	Cgroups, CpuMax, Cpufreq, LLCExtend, SMTWholeCore                   bool
}

func DefaultFeatures() Features {
	return Features{
		WakeupPreempt: true, Eligibility: true, EligibleScan: true,
		RunToParity: true, PreemptShort: true, PlaceLag: true,
		PlaceRelDeadline: true, DelayDequeue: true, DelayRequeue: true,
		ProxyExec: true, Hrtick: true, VrefUpdate: true, WakeSync: true,
		WaWeight: true, SISUtil: true, NewidleCost: true,
		NewidleSampling: true, CapacityPressure: true, Cgroups: true,
		CpuMax: true, Cpufreq: true,
	}
}

func (f *Features) Normalize() {
	if !f.Eligibility {
		f.EligibleScan, f.RunToParity = false, false
	}
	if !f.DelayDequeue {
		f.DelayRequeue = false
	}
	if !f.Cgroups {
		f.CpuMax = false
	}
}

func (p *Policy) placeTask(t *Task, cpu int32, sleep bool) {
	p.packs[cpu].placeWith(t, sleep, p.F.PlaceLag, p.F.PlaceRelDeadline)
}
