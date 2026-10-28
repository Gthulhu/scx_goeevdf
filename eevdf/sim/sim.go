// Package sim provides a deterministic, slice-driven CPU model for policy tests.
// It models dispatch, preemption, runtime accounting and periodic callbacks,
// not the kernel verifier or the full sched_ext callback ordering.
package sim

import (
	"fmt"

	"github.com/Gthulhu/scx_goeevdf/eevdf"
)

type Task struct {
	PID     int32
	Weight  uint64 // sched_ext units: nice 0 = 100
	Cgroup  uint64
	Allowed []int32
	Runtime uint64
	seq     uint64
}

type cpu struct {
	pid        int32
	start, end uint64
	seq        uint64
	pending    []eevdf.Cmd
}

type Simulator struct {
	Policy   *eevdf.Policy
	Now      uint64
	Tasks    map[int32]*Task
	cpus     []cpu
	cmds     []eevdf.Cmd
	nextTick uint64
}

func New(cpus int) (*Simulator, error) {
	if cpus <= 0 {
		return nil, fmt.Errorf("simulator requires at least one CPU")
	}
	s := &Simulator{Now: 1, Tasks: make(map[int32]*Task), cpus: make([]cpu, cpus), nextTick: 1_000_001}
	s.Policy = eevdf.New(s, cpus, eevdf.Enums{EnqWakeup: 1, EnqPreempt: 1 << 32})
	return s, nil
}

func (s *Simulator) Submit(cmd eevdf.Cmd) error {
	if cmd.Cpu < 0 || int(cmd.Cpu) >= len(s.cpus) {
		return fmt.Errorf("foreign simulator CPU %d", cmd.Cpu)
	}
	s.cmds = append(s.cmds, cmd)
	return nil
}
func (s *Simulator) Idle(cpu int32) bool             { return s.cpus[cpu].pid == 0 }
func (s *Simulator) CurrPid(cpu int32) int32         { return s.cpus[cpu].pid }
func (s *Simulator) RunStart(cpu int32) uint64       { return s.cpus[cpu].start }
func (s *Simulator) RunSeq(cpu int32) uint64         { return s.cpus[cpu].seq }
func (s *Simulator) SetQueued(int32, uint64, uint64) {}
func (s *Simulator) Affinity(pid int32) ([]int32, error) {
	t := s.Tasks[pid]
	if t == nil {
		return nil, fmt.Errorf("unknown simulator PID %d", pid)
	}
	return append([]int32(nil), t.Allowed...), nil
}

func (s *Simulator) Add(task Task, cpu int32) error {
	if task.PID <= 0 || task.Weight == 0 || s.Tasks[task.PID] != nil {
		return fmt.Errorf("invalid or duplicate simulator task")
	}
	if cpu < 0 || int(cpu) >= len(s.cpus) {
		return fmt.Errorf("invalid initial CPU")
	}
	if len(task.Allowed) == 0 {
		for c := range s.cpus {
			task.Allowed = append(task.Allowed, int32(c))
		}
	}
	found := false
	for _, c := range task.Allowed {
		if c < 0 || int(c) >= len(s.cpus) {
			return fmt.Errorf("invalid allowed CPU %d", c)
		}
		found = found || c == cpu
	}
	if !found {
		return fmt.Errorf("initial CPU is outside affinity")
	}
	task.Allowed = append([]int32(nil), task.Allowed...)
	s.Tasks[task.PID] = &task
	s.enqueue(&task, cpu)
	return s.flush()
}

func (s *Simulator) enqueue(t *Task, cpu int32) {
	t.seq++
	s.Policy.Handle(eevdf.Event{Kind: eevdf.EvEnqueue, Pid: t.PID, Cpu: cpu,
		Weight: t.Weight, Cgid: t.Cgroup, Now: s.Now, SumExec: t.Runtime, EnqSeq: t.seq})
}

func (s *Simulator) flush() error {
	for processed := 0; len(s.cmds) != 0; processed++ {
		if processed >= 10000 {
			return fmt.Errorf("simulator dispatch cycle at %d", s.Now)
		}
		cmd := s.cmds[0]
		s.cmds = s.cmds[1:]
		c := &s.cpus[cmd.Cpu]
		switch cmd.Kind {
		case eevdf.CmdDispatch:
			t := s.Tasks[cmd.Pid]
			if t == nil || t.seq != cmd.EnqSeq || cmd.Slice == 0 {
				return fmt.Errorf("invalid simulator dispatch: %+v", cmd)
			}
			for index := range s.cpus {
				if s.cpus[index].pid == cmd.Pid {
					return fmt.Errorf("dispatch of running simulator PID %d", cmd.Pid)
				}
			}
			preempt := cmd.EnqFlags&s.Policy.Enum.EnqPreempt != 0 && c.pid == cmd.ExpectCurr
			if preempt {
				c.pending = append([]eevdf.Cmd{cmd}, c.pending...)
			} else {
				c.pending = append(c.pending, cmd)
			}
			if preempt && c.pid != 0 {
				c.end = s.Now
			}
		case eevdf.CmdStop:
			if c.pid == cmd.Pid {
				c.end = s.Now
			}
		case eevdf.CmdKick:
			if c.pid != 0 && cmd.KickFlags&2 != 0 {
				c.end = s.Now
			}
		case eevdf.CmdHrtick:
			if c.pid != 0 && uint32(c.seq) == cmd.Slot {
				c.end = min(c.end, max(cmd.Slice, s.Now))
			}
		default:
			return fmt.Errorf("unknown simulator command %d", cmd.Kind)
		}
	}
	for index := range s.cpus {
		c := &s.cpus[index]
		if c.pid == 0 && len(c.pending) != 0 {
			cmd := c.pending[0]
			c.pending = c.pending[1:]
			c.pid, c.start, c.end = cmd.Pid, s.Now, s.Now+cmd.Slice
			c.seq++
		}
	}
	return nil
}

func (s *Simulator) Run(duration uint64) error {
	if duration == 0 || duration > ^uint64(0)-s.Now {
		return fmt.Errorf("invalid simulation duration")
	}
	until := s.Now + duration
	for s.Now < until {
		s.Policy.Schedule()
		if err := s.flush(); err != nil {
			return err
		}
		next := min(until, s.nextTick)
		for _, c := range s.cpus {
			if c.pid != 0 {
				next = min(next, c.end)
			}
		}
		for _, c := range s.cpus {
			if c.pid != 0 {
				s.Tasks[c.pid].Runtime += next - s.Now
			}
		}
		s.Now = next
		for index := range s.cpus {
			c := &s.cpus[index]
			if c.pid != 0 && c.end <= s.Now {
				task := s.Tasks[c.pid]
				c.pid = 0
				s.enqueue(task, int32(index))
			}
		}
		if s.Now >= s.nextTick {
			s.Policy.Periodic(s.Now)
			s.Policy.BalanceTick(s.Now)
			s.nextTick = s.Now + 1_000_000
		}
	}
	return nil
}
