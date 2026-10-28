package eevdf

// Task is the fair.c sched_entity state owned by the Go scheduling loop.
type Task struct {
	Pid, Tgid   int32
	Cpu         int32
	EnqSeq      uint64
	Weight      uint64
	Nice        uint64
	Cgid        uint64
	CustomSlice uint64
	Policy      uint32
	Allowed     []int32
	Vruntime    uint64
	Req         Request
	Slot        uint32
	Blocked     bool

	seq     uint64
	node    *node
	pack    *Pack
	joined  uint64
	lag     int64
	delay   delayRec
	initial bool
	state   State
	lastSum uint64
	backlog *bwGroup
	wake    wakeState
}

type State uint8

const (
	Sleeping State = iota
	Queued
	Dispatched
	Parked
)

type delayRec struct {
	pack     *Pack
	ref, sum uint64
	gen      uint64
}
