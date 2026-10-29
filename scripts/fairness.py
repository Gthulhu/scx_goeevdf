#!/usr/bin/env python3
"""Measure nice-0/nice-6 CPU time on one CPU; run in the test VM."""

import argparse
import json
import math
from pathlib import Path
import os
import selectors
import subprocess
import sys
import time


def cpu_time(pid):
    stat = Path(f"/proc/{pid}/stat").read_text()
    fields = stat[stat.rindex(")") + 2:].split()
    return int(fields[11]) + int(fields[12])


def wait_ready(workers, cpu, timeout=30):
    with selectors.DefaultSelector() as selector:
        for worker, nice in workers:
            selector.register(worker.stdout, selectors.EVENT_READ, (worker, nice))
        deadline = time.monotonic() + timeout
        while selector.get_map():
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise RuntimeError("timed out waiting for fairness workers")
            for key, _ in selector.select(remaining):
                worker, nice = key.data
                line = worker.stdout.readline()
                if not line:
                    raise RuntimeError(f"worker {worker.pid} exited before readiness")
                ready = json.loads(line)
                expected = {"pid": worker.pid, "nice": nice, "cpu": cpu}
                if ready != expected:
                    raise RuntimeError(f"worker readiness {ready!r}, expected {expected!r}")
                selector.unregister(key.fileobj)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--worker", action="store_true")
    parser.add_argument("--nice", type=int, default=0)
    parser.add_argument("--seconds", type=float, default=6)
    parser.add_argument("--cpu", type=int)
    args = parser.parse_args()
    if not math.isfinite(args.seconds) or args.seconds <= 0:
        parser.error("--seconds must be finite and positive")
    allowed = os.sched_getaffinity(0)
    cpu = min(allowed) if args.cpu is None else args.cpu
    if cpu not in allowed:
        parser.error("--cpu must be within the current affinity mask")
    if args.worker:
        os.sched_setaffinity(0, {cpu})
        os.nice(args.nice)
        print(json.dumps({"pid": os.getpid(),
                          "nice": os.getpriority(os.PRIO_PROCESS, 0), "cpu": cpu}),
              flush=True)
        value = 1
        while True:
            value = (value * 17 + 3) % 1000000007

    workers = []
    try:
        for nice in (0, 6):
            worker = subprocess.Popen(
                [sys.executable, __file__, "--worker", "--nice", str(nice),
                 "--cpu", str(cpu)], stdout=subprocess.PIPE, text=True)
            workers.append((worker, nice))
        wait_ready(workers, cpu)
        time.sleep(1)
        before = [cpu_time(worker.pid) for worker, _ in workers]
        time.sleep(args.seconds)
        after = [cpu_time(worker.pid) for worker, _ in workers]
        elapsed = [a - b for a, b in zip(after, before)]
        if min(elapsed) <= 0:
            raise RuntimeError(f"workers did not make progress: {elapsed}")
        ratio = elapsed[0] / elapsed[1]
        print(f"nice 0 / nice 6 CPU-time ratio: {ratio:.3f}; ticks={elapsed}", flush=True)
        if not 2.5 <= ratio <= 5.0:
            raise RuntimeError(f"fairness ratio {ratio:.3f} outside [2.5, 5.0]")
    finally:
        stuck = []
        for worker, _ in workers:
            if worker.poll() is None:
                worker.terminate()
        for worker, _ in workers:
            try:
                worker.wait(timeout=5)
            except subprocess.TimeoutExpired:
                worker.kill()
                worker.wait()
                stuck.append(worker.pid)
            finally:
                worker.stdout.close()
        if stuck:
            raise RuntimeError(f"workers did not terminate: {stuck}")


if __name__ == "__main__":
    main()
