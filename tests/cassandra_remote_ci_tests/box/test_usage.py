# SPDX-License-Identifier: Apache-2.0
"""The box's CPU and memory use, and the peak memory of each job's containers."""

import subprocess
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from cassandra_remote_ci.box import summary
from cassandra_remote_ci.box.usage import Sampler, add_sample, memory_bytes
from cassandra_remote_ci.shared.state import run_files, write_json

GIB = 2**30
USAGE = {"cpu": 0.93, "cpu_average": 0.81, "cpu_peak": 0.97, "memory_used": 61 * GIB, "memory_average": 25 * GIB,
         "memory_peak": int(40.5 * GIB), "memory_total": 94 * GIB, "samples": 600}


class UsageTest(unittest.TestCase):
    def test_samples_add_up_to_averages_and_peaks(self):
        usage = {}
        for busy, used in ((0.5, 20), (1.0, 40), (0.6, 30)):
            usage = add_sample(usage, busy, {"memory_used": used * GIB, "memory_total": 90 * GIB}, 1.0)

        self.assertEqual({"updated": 1.0, "cpu": 0.6, "cpu_average": 0.7, "cpu_peak": 1.0, "memory_used": 30 * GIB,
                          "memory_average": 30 * GIB, "memory_peak": 40 * GIB, "memory_total": 90 * GIB, "samples": 3},
                         {**usage, "cpu_average": round(usage["cpu_average"], 6)})

    def test_a_retry_carries_on_the_averages_of_the_attempt_before(self):
        usage = add_sample({"cpu_average": 0.5, "cpu_peak": 0.9, "memory_average": 20 * GIB, "memory_peak": 50 * GIB,
                            "samples": 3}, 0.9, {"memory_used": 40 * GIB, "memory_total": 90 * GIB}, 1.0)

        self.assertEqual((0.6, 0.9, 25 * GIB, 50 * GIB, 4),
                         (round(usage["cpu_average"], 6), usage["cpu_peak"], usage["memory_average"],
                          usage["memory_peak"], usage["samples"]))

    def test_a_sample_without_cpu_or_memory_only_updates_what_it_has(self):
        before = {"cpu_average": 0.5, "samples": 3, "memory_used": 10 * GIB}

        self.assertEqual({**before, "updated": 2.0}, add_sample(before, None, {}, 2.0))

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
        self.assertIn("| box while jobs ran | average | peak | total |\n|---|---|---|---|\n"
                      "| CPU | 81% | 97% | - |\n| memory | 25.0G | 40.5G | 94.0G |\n", text)
        self.assertIn("| dtest | 64/64 | 1051 | 1107 | 0 | 0 | 276 | 15.0G | 7.2G |", text)

    def test_summary_of_a_run_sampled_before_memory_averages(self):
        files = run_files(Path(tempfile.mkdtemp(prefix="cassandra-remote-ci-test-", dir="/tmp")), "r1")
        write_json(files.usage, {"cpu": 0.93, "cpu_average": 0.81, "memory_used": 61 * GIB, "memory_total": 94 * GIB})
        summary.write_summary(files, {"run_id": "r1", "phase": "done", "elapsed": 60, "groups": []}, [], [])

        self.assertIn("| CPU | 81% | - | - |\n| memory | - | - | 94.0G |\n", files.summary_md.read_text())


if __name__ == "__main__":
    unittest.main()
