package bpf

/*
#include "bridge.h"
*/
import "C"
import "unsafe"

const (
	MaxCPUs   = 1024
	MaxTasks  = 65536
	MaxShards = 32

	EvEnqueue    = 1
	EvSleep      = 2
	EvDequeue    = 3
	EvExit       = 4
	EvKeep       = 5
	EvIdle       = 6
	EvYield      = 7
	EvCpumask    = 8
	EvCgrpInit   = 9
	EvCgrpExit   = 10
	EvCgrpMove   = 11
	EvCgrpWeight = 12
	EvCgrpIdle   = 13
	EvCgrpBW     = 14

	CmdDispatch = 1
	CmdKick     = 2
	CmdStop     = 3
	CmdHrtick   = 4
	CmdfHrtick  = 1
)

type Ev struct {
	Kind                                           uint32
	Pid, Tgid, Cpu                                 int32
	Flags, Now, SumExec, Weight, CustomSlice, Cgid uint64
	WakerPid, WakerCpu                             int32
	WakeFlags, Policy, NrCpusAllowed, Slot         uint32
	EnqSeq                                         uint64
	Arg                                            [4]uint64
}

// Event is the public name for the 128-byte wire event; Ev remains for callers
// using the plan's shorter spelling.
type Event = Ev

type Cmd struct {
	Kind                    uint32
	Pid, Cpu                int32
	Cflags                  uint32
	Slice, EnqFlags, EnqSeq uint64
	Slot                    uint32
	ExpectCurr              int32
	KickFlags               uint64
	_                       uint64
}

type CpuSlot struct {
	CurrPid, Idle                                                    uint32
	RunStart, RunSeq, DisplacedNs, ReleasedAt, Keeps, PerfApplied, _ uint64
}
type CpuCtl struct {
	NrQueued, KeepSlice, PerfTarget uint64
	_                               [5]uint64
}
type TaskPub struct {
	Seq                           uint64
	Pid                           int32
	_                             uint32
	CoreVtime, Vruntime, Deadline uint64
	Lag                           int64
	Weight, PublishedAt           uint64
}

var (
	_ [unsafe.Sizeof(Ev{}) - C.sizeof_struct_ev]byte
	_ [C.sizeof_struct_ev - unsafe.Sizeof(Ev{})]byte
	_ [unsafe.Sizeof(Cmd{}) - C.sizeof_struct_cmd]byte
	_ [C.sizeof_struct_cmd - unsafe.Sizeof(Cmd{})]byte
	_ [unsafe.Sizeof(CpuSlot{}) - C.sizeof_struct_cpu_slot]byte
	_ [C.sizeof_struct_cpu_slot - unsafe.Sizeof(CpuSlot{})]byte
	_ [unsafe.Sizeof(CpuCtl{}) - C.sizeof_struct_cpu_ctl]byte
	_ [C.sizeof_struct_cpu_ctl - unsafe.Sizeof(CpuCtl{})]byte
	_ [unsafe.Sizeof(TaskPub{}) - C.sizeof_struct_task_pub]byte
	_ [C.sizeof_struct_task_pub - unsafe.Sizeof(TaskPub{})]byte
	_ [unsafe.Offsetof(Ev{}.SumExec) - 32]byte
	_ [32 - unsafe.Offsetof(Ev{}.SumExec)]byte
	_ [unsafe.Offsetof(Ev{}.EnqSeq) - 88]byte
	_ [88 - unsafe.Offsetof(Ev{}.EnqSeq)]byte
	_ [unsafe.Offsetof(Cmd{}.EnqSeq) - 32]byte
	_ [32 - unsafe.Offsetof(Cmd{}.EnqSeq)]byte
)
