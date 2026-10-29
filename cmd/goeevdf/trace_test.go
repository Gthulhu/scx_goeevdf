package main

import (
	"bufio"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"sync"
	"syscall"
	"testing"

	"github.com/Gthulhu/scx_goeevdf/eevdf"
	"github.com/Gthulhu/scx_goeevdf/internal/bpf"
)

func TestRuntimeTraceRecords(t *testing.T) {
	path := filepath.Join(t.TempDir(), "trace.jsonl")
	trace, err := openTrace(path)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := openTrace(path); !errors.Is(err, os.ErrExist) {
		t.Fatalf("trace overwrote existing output: %v", err)
	}
	var wg sync.WaitGroup
	for shard := 0; shard < 2; shard++ {
		wg.Add(1)
		go func(pid int32) {
			defer wg.Done()
			for i := 0; i < 10; i++ {
				trace.record(traceRecord{Time: uint64(i), Event: &bpf.Ev{Kind: bpf.EvEnqueue, Pid: pid}})
				trace.record(traceRecord{Command: &eevdf.Cmd{Kind: eevdf.CmdDispatch, Pid: pid}})
				trace.record(traceRecord{Task: &eevdf.TaskPub{Pid: pid, Weight: 276}})
			}
		}(int32(shard + 1))
	}
	wg.Wait()
	if err := trace.check(); err != nil {
		t.Fatal(err)
	}
	if err := trace.close(); err != nil {
		t.Fatal(err)
	}
	info, err := os.Stat(path)
	if err != nil || info.Mode().Perm() != 0600 {
		t.Fatalf("trace permissions: %v %v", info, err)
	}
	file, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	decoder := json.NewDecoder(file)
	counts := [3]int{}
	for {
		var record traceRecord
		err := decoder.Decode(&record)
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		if record.Event != nil {
			counts[0]++
		}
		if record.Command != nil {
			counts[1]++
		}
		if record.Task != nil {
			counts[2]++
		}
	}
	if counts != [3]int{20, 20, 20} {
		t.Fatalf("lost records across owners: %v", counts)
	}
}

type failedTraceWriter struct{}

func (failedTraceWriter) Write([]byte) (int, error) { return 0, syscall.ENOSPC }

func TestRuntimeTraceErrorsAndDisabled(t *testing.T) {
	trace, err := openTrace("")
	if err != nil || trace != nil {
		t.Fatalf("disabled tracing: %v %v", trace, err)
	}
	trace.record(traceRecord{})
	if err := trace.check(); err != nil {
		t.Fatal(err)
	}
	if err := trace.close(); err != nil {
		t.Fatal(err)
	}
	trace, err = openTrace(filepath.Join(t.TempDir(), "trace.jsonl"))
	if err != nil {
		t.Fatal(err)
	}
	trace.buf = bufio.NewWriterSize(failedTraceWriter{}, 1)
	trace.record(traceRecord{Time: 123})
	if !errors.Is(trace.check(), syscall.ENOSPC) {
		t.Fatalf("write failure hidden: %v", trace.check())
	}
	if err := trace.close(); !errors.Is(err, syscall.ENOSPC) {
		t.Fatalf("close hid write failure: %v", err)
	}
}
