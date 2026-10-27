package main

import (
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/Gthulhu/scx_goeevdf/eevdf"
)

type groupSync struct {
	last    map[uint64]time.Time
	applied map[uint64]groupConfig
	paths   map[uint64]string
}

type groupConfig struct {
	parent, weight, quota, period uint64
}

func cgroupPath(pid int32) (string, error) {
	data, err := os.ReadFile(fmt.Sprintf("/proc/%d/cgroup", pid))
	if err != nil {
		return "", err
	}
	for _, line := range strings.Split(string(data), "\n") {
		if strings.HasPrefix(line, "0::/") {
			rel := strings.TrimPrefix(line, "0::/")
			path := filepath.Join("/sys/fs/cgroup", rel)
			if path != "/sys/fs/cgroup" && !strings.HasPrefix(path, "/sys/fs/cgroup/") {
				return "", fmt.Errorf("cgroup path outside cgroupfs")
			}
			return path, nil
		}
	}
	return "", fmt.Errorf("no unified cgroup for pid %d", pid)
}

func cgroupID(path string) (uint64, error) {
	info, err := os.Stat(path)
	if err != nil {
		return 0, err
	}
	return info.Sys().(*syscall.Stat_t).Ino, nil
}

func readWeight(path string) uint64 {
	data, err := os.ReadFile(filepath.Join(path, "cpu.weight"))
	if err == nil {
		if w, err := strconv.ParseUint(strings.TrimSpace(string(data)), 10, 64); err == nil {
			return w
		}
	}
	return 100
}

func readQuota(path string) (quota, period uint64, err error) {
	data, err := os.ReadFile(filepath.Join(path, "cpu.max"))
	if err != nil {
		return 0, 0, err
	}
	fields := strings.Fields(string(data))
	if len(fields) != 2 {
		return 0, 0, fmt.Errorf("invalid cpu.max in %s", path)
	}
	period, err = strconv.ParseUint(fields[1], 10, 64)
	if err != nil || period == 0 || period > ^uint64(0)/1000 {
		return 0, 0, fmt.Errorf("invalid cpu.max period in %s", path)
	}
	if fields[0] == "max" {
		return 0, period * 1000, nil
	}
	quota, err = strconv.ParseUint(fields[0], 10, 64)
	if err != nil || quota > ^uint64(0)/1000 {
		return 0, 0, fmt.Errorf("invalid cpu.max quota in %s", path)
	}
	return quota * 1000, period * 1000, nil
}

func (s *groupSync) configure(p *eevdf.Policy, path string) error {
	if path == "/sys/fs/cgroup" {
		return nil
	}
	id, err := cgroupID(path)
	if err != nil {
		return err
	}
	parent := filepath.Dir(path)
	if err := s.configure(p, parent); err != nil {
		return err
	}
	parentID := uint64(0)
	if parent != "/sys/fs/cgroup" {
		parentID, err = cgroupID(parent)
		if err != nil {
			return err
		}
	}
	config := groupConfig{parent: parentID, weight: readWeight(path)}
	if quota, period, err := readQuota(path); err == nil {
		config.quota, config.period = quota, period
	}
	previous, known := s.applied[id]
	if known && previous == config {
		return nil
	}
	if err := p.SetCgroup(id, parentID, config.weight); err != nil {
		return err
	}
	if config.period != 0 && (!known || previous.quota != config.quota || previous.period != config.period) {
		if err := p.SetCPUQuota(id, config.quota, config.period); err != nil {
			return err
		}
	} else if config.period == 0 && known && previous.period != 0 {
		if err := p.SetCPUQuota(id, 0, 0); err != nil {
			return err
		}
	}
	if s.applied == nil {
		s.applied = make(map[uint64]groupConfig)
	}
	s.applied[id] = config
	if s.paths == nil {
		s.paths = make(map[uint64]string)
	}
	s.paths[id] = path
	return nil
}

func (s *groupSync) refresh(p *eevdf.Policy, id uint64) {
	path := s.paths[id]
	if path == "" {
		return
	}
	if actual, err := cgroupID(path); err == nil && actual == id {
		if s.configure(p, path) == nil {
			s.last[id] = time.Now()
		}
	}
}

func (s *groupSync) sync(p *eevdf.Policy, pid int32, id uint64) {
	if id == 0 || time.Since(s.last[id]) < time.Second {
		return
	}
	path, err := cgroupPath(pid)
	if err != nil {
		return
	}
	got, err := cgroupID(path)
	if err != nil || got != id {
		return
	}
	if err := s.configure(p, path); err == nil {
		if s.last == nil {
			s.last = make(map[uint64]time.Time)
		}
		s.last[id] = time.Now()
	}
}
