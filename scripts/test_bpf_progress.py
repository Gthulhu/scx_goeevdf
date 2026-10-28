#!/usr/bin/env python3
"""Compile actual BPF callback bodies against deterministic mock helpers."""

import os
from pathlib import Path
import re
import subprocess
import tempfile


ROOT = Path(__file__).resolve().parents[1]
SOURCE = ROOT / "bpf/goeevdf.bpf.c"


def callback(source, name):
    match = re.search(
        rf"^[^\n;]*\b{re.escape(name)}\s*(?:\(|,)", source, re.MULTILINE
    )
    if match is None:
        raise ValueError(f"callback {name} not found")
    start = source.index("{", match.start())
    depth = 1
    end = start + 1
    while depth:
        if source[end] == "{":
            depth += 1
        elif source[end] == "}":
            depth -= 1
        end += 1
    return source[match.start():end] + "\n"


def main():
    source = SOURCE.read_text()
    names = (
        "ev_reserve", "ev_task", "send_task", "ge_select_cpu", "ge_enqueue",
        "hrtick_fire", "heartbeat_fire", "do_dispatch", "ge_dispatch",
        "core_vtime", "ge_core_sched_before", "ge_cpu_release", "ge_cpu_acquire",
    )
    with tempfile.TemporaryDirectory(prefix="goeevdf-bpf-test-") as directory:
        generated = Path(directory) / "callbacks.h"
        generated.write_text("\n".join(callback(source, name) for name in names))
        binary = Path(directory) / "bpf-progress"
        subprocess.run(
            [
                os.environ.get("CLANG", "clang"), "-O2", "-std=gnu11",
                "-I", str(ROOT), "-I", directory,
                str(ROOT / "scripts/bpf_progress.c"), "-o", str(binary),
            ],
            check=True,
        )
        subprocess.run([str(binary)], check=True)


if __name__ == "__main__":
    main()
