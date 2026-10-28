package sim

import (
	"math"
	"testing"
)

func TestWeightedServiceThreshold(t *testing.T) {
	var first [2]uint64
	for attempt := 0; attempt < 2; attempt++ {
		s, err := New(1)
		if err != nil {
			t.Fatal(err)
		}
		for _, task := range []Task{{PID: 1, Weight: 100}, {PID: 2, Weight: 305}} {
			if err := s.Add(task, 0); err != nil {
				t.Fatal(err)
			}
		}
		if err := s.Run(6_000_000_000); err != nil {
			t.Fatal(err)
		}
		ratio := float64(s.Tasks[2].Runtime) / float64(s.Tasks[1].Runtime)
		want := 3121.0 / 1024
		if math.Abs(ratio/want-1) > 0.05 {
			t.Fatalf("weighted service %.4f, want %.4f ±5%%", ratio, want)
		}
		got := [2]uint64{s.Tasks[1].Runtime, s.Tasks[2].Runtime}
		if attempt == 0 {
			first = got
		} else if got != first {
			t.Fatalf("simulation is not deterministic: %v != %v", got, first)
		}
	}
}

func TestAffinityAndServiceConservation(t *testing.T) {
	s, _ := New(2)
	for pid := int32(1); pid <= 4; pid++ {
		cpu := (pid - 1) % 2
		if err := s.Add(Task{PID: pid, Weight: 100, Allowed: []int32{cpu}}, cpu); err != nil {
			t.Fatal(err)
		}
	}
	const duration = 1_000_000_000
	if err := s.Run(duration); err != nil {
		t.Fatal(err)
	}
	var sum uint64
	for pid, task := range s.Tasks {
		sum += task.Runtime
		if task.Runtime < duration/2-s.Policy.Slice || task.Runtime > duration/2+s.Policy.Slice {
			t.Fatalf("PID %d runtime %d outside equal-service bound", pid, task.Runtime)
		}
	}
	if sum != 2*duration {
		t.Fatalf("lost or duplicated CPU service: %d != %d", sum, 2*duration)
	}
	for index, cpu := range s.cpus {
		if cpu.pid != 0 && s.Tasks[cpu.pid].Allowed[0] != int32(index) {
			t.Fatalf("task %d escaped affinity to CPU %d", cpu.pid, index)
		}
	}
}

func TestCgroupServiceThreshold(t *testing.T) {
	s, _ := New(4)
	for id, weight := range map[uint64]uint64{1: 200, 2: 100} {
		if err := s.Policy.SetCgroup(id, 0, weight); err != nil {
			t.Fatal(err)
		}
	}
	for id := uint64(1); id <= 2; id++ {
		for cpu := int32(0); cpu < 4; cpu++ {
			task := Task{PID: int32(id)*10 + cpu, Weight: 100, Cgroup: id}
			if err := s.Add(task, cpu); err != nil {
				t.Fatal(err)
			}
		}
	}
	if err := s.Run(6_000_000_000); err != nil {
		t.Fatal(err)
	}
	var service [3]uint64
	for _, task := range s.Tasks {
		service[task.Cgroup] += task.Runtime
	}
	ratio := float64(service[1]) / float64(service[2])
	if math.Abs(ratio/2-1) > 0.05 {
		t.Fatalf("cgroup service %.4f, want 2 ±5%%", ratio)
	}
}

func TestQuotaServiceThreshold(t *testing.T) {
	s, _ := New(4)
	if err := s.Policy.SetCgroup(1, 0, 100); err != nil {
		t.Fatal(err)
	}
	const quota, period = 50_000_000, 100_000_000
	if err := s.Policy.SetCPUQuota(1, quota, period); err != nil {
		t.Fatal(err)
	}
	for cpu := int32(0); cpu < 4; cpu++ {
		if err := s.Add(Task{PID: cpu + 1, Weight: 100, Cgroup: 1}, cpu); err != nil {
			t.Fatal(err)
		}
	}
	var before uint64
	for i := 0; i < 10; i++ {
		if err := s.Run(period); err != nil {
			t.Fatal(err)
		}
		var total uint64
		for _, task := range s.Tasks {
			total += task.Runtime
		}
		if service := total - before; service > quota+s.Policy.Slice || service < quota-s.Policy.Slice {
			t.Fatalf("period %d service=%d outside quota ± one slice", i, service)
		}
		before = total
	}
}
