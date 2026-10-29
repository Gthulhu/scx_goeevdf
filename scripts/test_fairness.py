#!/usr/bin/env python3
"""Unprivileged regression tests for the fairness worker readiness barrier."""

import os
import subprocess
import sys
import time
import unittest
from unittest.mock import patch

import fairness


class ReadinessTests(unittest.TestCase):
    def worker(self, code):
        worker = subprocess.Popen(
            [sys.executable, "-c", code], stdout=subprocess.PIPE, text=True)
        self.addCleanup(worker.stdout.close)

        def stop():
            if worker.poll() is None:
                worker.terminate()
            worker.wait(timeout=5)

        self.addCleanup(stop)
        return worker

    def test_waits_for_both_workers_including_delayed_startup(self):
        cpu = min(os.sched_getaffinity(0))
        started = time.monotonic()
        code = (
            "import json,os,time; time.sleep({delay}); "
            "print(json.dumps(dict(pid=os.getpid(),nice={nice},cpu={cpu})),flush=True)"
        )
        slow = self.worker(code.format(delay=.2, nice=6, cpu=cpu))
        fast = self.worker(code.format(delay=0, nice=0, cpu=cpu))
        fairness.wait_ready([(slow, 6), (fast, 0)], cpu, timeout=5)
        self.assertGreaterEqual(time.monotonic() - started, .2)
        self.assertEqual(slow.stdout.read(), "")
        self.assertEqual(fast.stdout.read(), "")

    def test_real_worker_sets_priority_and_affinity_before_ready(self):
        cpu = min(os.sched_getaffinity(0))
        worker = subprocess.Popen(
            [sys.executable, fairness.__file__, "--worker", "--nice", "6",
             "--cpu", str(cpu)], stdout=subprocess.PIPE, text=True)
        self.addCleanup(worker.stdout.close)

        def stop():
            worker.terminate()
            worker.wait(timeout=5)

        self.addCleanup(stop)
        expected = min(os.getpriority(os.PRIO_PROCESS, 0) + 6, 19)
        fairness.wait_ready([(worker, expected)], cpu, timeout=5)
        self.assertEqual(os.getpriority(os.PRIO_PROCESS, worker.pid), expected)
        self.assertEqual(os.sched_getaffinity(worker.pid), {cpu})

    def test_invalid_configuration_is_not_ready(self):
        worker = self.worker(
            "import json,os; print(json.dumps(dict(pid=os.getpid(),nice=0,cpu=0)))")
        with self.assertRaisesRegex(RuntimeError, "worker readiness"):
            fairness.wait_ready([(worker, 6)], 0, timeout=5)

    def test_early_exit_is_reported(self):
        worker = self.worker("pass")
        with self.assertRaisesRegex(RuntimeError, "exited before readiness"):
            fairness.wait_ready([(worker, 0)], 0, timeout=5)

    def test_missing_readiness_times_out(self):
        worker = self.worker("import time; time.sleep(10)")
        with self.assertRaisesRegex(RuntimeError, "timed out"):
            fairness.wait_ready([(worker, 0)], 0, timeout=.05)

    def test_failed_barrier_never_samples_cpu_time(self):
        worker = self.worker("pass")
        with patch.object(fairness.subprocess, "Popen", return_value=worker), \
             patch.object(fairness, "wait_ready", side_effect=RuntimeError("not ready")), \
             patch.object(fairness, "cpu_time") as sample, \
             patch.object(sys, "argv", ["fairness.py"]):
            with self.assertRaisesRegex(RuntimeError, "not ready"):
                fairness.main()
            sample.assert_not_called()


if __name__ == "__main__":
    unittest.main()
