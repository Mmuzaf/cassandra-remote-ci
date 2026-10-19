# SPDX-License-Identifier: Apache-2.0
"""The box's CPU and memory use, and the peak memory of each job's containers."""

import subprocess
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from cassandra_remote_ci.box import summary
from cassandra_remote_ci.box.usage import Sampler, memory_bytes
from cassandra_remote_ci.shared.state import run_files, write_json

USAGE = {"cpu": 0.93, "cpu_average": 0.81, "memory_used": 61 * 2**30, "memory_total": 94 * 2**30}


class UsageTest(unittest.TestCase):
    def test_docker_stats_sizes(self):
        self.assertEqual((1536 * 2**20, 512 * 2**10, 2 * 10**9, 0),
                         tuple(memory_bytes(text) for text in ("1.5GiB", "512KiB ", "2GB", "-")))

    def test_the_peak_of_a_running_job_is_the_highest_memory_its_containers_used(self):
        files = run_files(Path(tempfile.mkdtemp(prefix="cassandra-remote-ci-test-", dir="/tmp")), "r1")
        sampler = Sampler(files.usage)
        work_dir = files.job_dir("dtest_1_64")
        work_dir.mkdir(parents=True)
        (work_dir / "containers").write_text("abc123\n\n")
        sampler.watch(work_dir)

        for usage in ("2.5GiB / 15GiB", "7GiB / 15GiB", "3GiB / 15GiB"):
            stats_output = subprocess.CompletedProcess([], 0, stdout=f"abc123def {usage}\n")
            with patch("subprocess.run", return_value=stats_output) as docker:
                sampler.sample_containers()

        self.assertEqual(["abc123"], docker.call_args.args[0][-1:])
        self.assertEqual(7 * 2**30, sampler.peak_memory(work_dir))
        self.assertIsNone(sampler.peak_memory(work_dir))

    def test_summary_shows_the_box_usage_and_memory_per_group(self):
        files = run_files(Path(tempfile.mkdtemp(prefix="cassandra-remote-ci-test-", dir="/tmp")), "r1")
        write_json(files.usage, USAGE)
        row = {"group": "dtest", "jobs": 64, "jobs_total": 64, "classes": 1051, "tests": 1107, "failures": 0,
               "errors": 0, "skipped": 276, "memory": 15 * 2**30, "peak_memory": int(7.2 * 2**30)}
        summary.write_summary(files, {"run_id": "r1", "phase": "done", "elapsed": 60, "groups": [row]}, [], [])

        text = files.summary_md.read_text()
        self.assertIn("The box's CPUs were 81% busy on average while jobs ran.", text)
        self.assertIn("| dtest | 64/64 | 1051 | 1107 | 0 | 0 | 276 | 15.0G | 7.2G |", text)


if __name__ == "__main__":
    unittest.main()
