#!/usr/bin/env python3
"""Measure nice-0/nice-6 CPU time on one CPU; run in the test VM."""

import argparse
from pathlib import Path
import os
import subprocess
import sys
import time


def cpu_time(pid):
    stat = Path(f"/proc/{pid}/stat").read_text()
    fields = stat[stat.rindex(")") + 2:].split()
    return int(fields[11]) + int(fields[12])


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--worker", action="store_true")
    parser.add_argument("--nice", type=int, default=0)
    parser.add_argument("--seconds", type=float, default=6)
    args = parser.parse_args()
    if args.seconds <= 0:
        parser.error("--seconds must be positive")
    if args.worker:
        os.sched_setaffinity(0, {min(os.sched_getaffinity(0))})
        os.nice(args.nice)
        value = 1
        while True:
            value = (value * 17 + 3) % 1000000007

    workers = []
    try:
        for nice in (0, 6):
            workers.append(subprocess.Popen(
                [sys.executable, __file__, "--worker", "--nice", str(nice)]
            ))
        time.sleep(1)
        before = [cpu_time(worker.pid) for worker in workers]
        time.sleep(args.seconds)
        after = [cpu_time(worker.pid) for worker in workers]
        elapsed = [a - b for a, b in zip(after, before)]
        if min(elapsed) <= 0:
            raise RuntimeError(f"workers did not make progress: {elapsed}")
        ratio = elapsed[0] / elapsed[1]
        print(f"nice 0 / nice 6 CPU-time ratio: {ratio:.3f}; ticks={elapsed}", flush=True)
        if not 2.5 <= ratio <= 5.0:
            raise RuntimeError(f"fairness ratio {ratio:.3f} outside [2.5, 5.0]")
    finally:
        for worker in workers:
            worker.terminate()
        for worker in workers:
            worker.wait(timeout=5)


if __name__ == "__main__":
    main()
