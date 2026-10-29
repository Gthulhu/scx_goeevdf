package main

import (
	"bufio"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"sync"

	"github.com/Gthulhu/scx_goeevdf/eevdf"
	"github.com/Gthulhu/scx_goeevdf/internal/bpf"
)

// runtimeTrace is opt-in diagnostics; disk I/O can perturb scheduling latency.
type runtimeTrace struct {
	mu   sync.Mutex
	file *os.File
	buf  *bufio.Writer
	err  error
}

type traceRecord struct {
	Time    uint64         `json:"time"`
	Event   *bpf.Ev        `json:"event,omitempty"`
	Command *eevdf.Cmd     `json:"command,omitempty"`
	Task    *eevdf.TaskPub `json:"task,omitempty"`
}

func openTrace(path string) (*runtimeTrace, error) {
	if path == "" {
		return nil, nil
	}
	file, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
	if err != nil {
		return nil, err
	}
	return &runtimeTrace{file: file, buf: bufio.NewWriterSize(file, 64*1024)}, nil
}

func (t *runtimeTrace) record(record traceRecord) {
	if t == nil {
		return
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.err == nil {
		t.err = json.NewEncoder(t.buf).Encode(record)
	}
}

func (t *runtimeTrace) check() error {
	if t == nil {
		return nil
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.err
}

func (t *runtimeTrace) close() error {
	if t == nil {
		return nil
	}
	t.err = errors.Join(t.err, t.buf.Flush(), t.file.Close())
	if t.err != nil {
		return fmt.Errorf("scheduler trace: %w", t.err)
	}
	return nil
}
