# SPDX-License-Identifier: Apache-2.0
"""Job statistics in stats/jobs.jsonl and the ETA computed from them."""

import io
import shutil
import tempfile
import time
import unittest
from contextlib import redirect_stdout
from pathlib import Path

from cassandra_remote_ci.box import progress, stats
from cassandra_remote_ci.box.orchestrate import Orchestrator
from cassandra_remote_ci.shared.plugin import Job
from cassandra_remote_ci.shared.state import context_for, run_files, write_json
from cassandra_remote_ci_tests.box.test_orchestrate import FakePlugin

INFO = {"plugin": "fake", "jdk": "17", "memory": "1g", "parallel": 2, "profile": ""}


def new_base() -> Path:
    base_dir = Path(tempfile.mkdtemp(prefix="cassandra-remote-ci-test-", dir="/tmp"))
    (base_dir / "src").mkdir()
    return base_dir


def write_run(base_dir: Path, run_id: str, **overrides) -> dict:
    info = {**INFO, "run_id": run_id, "trash_dir": str(base_dir / "trash"), **overrides}
    write_json(run_files(base_dir, run_id).info, info)
    return info


def job(run_id, job_id, group, seconds, jdk="17", crashed=False, tests=1):
    return {"run_id": run_id, "plugin": "fake", "profile": "", "jdk": jdk, "type": "job", "job": job_id,
            "group": group, "start": 0.0, "end": float(seconds), "crashed": crashed, "tests": tests}


class StatsFileTest(unittest.TestCase):
    def test_runs_append_their_jobs_and_prepare_and_survive_cleanup(self):
        base_dir = new_base()
        for run_id in ("r1", "r2"):
            with redirect_stdout(io.StringIO()):
                Orchestrator(FakePlugin(), context_for(base_dir, write_run(base_dir, run_id))).run()

        records = stats.read_stats(base_dir)
        self.assertEqual(["prepare", "job", "job", "job"] * 2, [record["type"] for record in records])
        self.assertEqual(["r1"] * 4 + ["r2"] * 4, [record["run_id"] for record in records])
        crash = next(record for record in records if record.get("job") == "crash_1_1")
        self.assertTrue(crash["crashed"])
        self.assertEqual(2, next(record for record in records if record.get("job") == "pass_1_1")["tests"])

        shutil.move(str(base_dir / "runs"), str(base_dir / "moved-runs"))
        self.assertEqual(records, stats.read_stats(base_dir))

    def test_a_job_that_ends_because_of_abort_leaves_no_record(self):
        base_dir = new_base()
        info = write_run(base_dir, "r1")
        plugin = FakePlugin()
        orchestrator = Orchestrator(plugin, context_for(base_dir, info))

        def killed(ctx, slot_dir, job, job_dir, classes=None):
            orchestrator.aborting = True
            return 137

        plugin.run_job = killed
        with redirect_stdout(io.StringIO()):
            orchestrator.run_one(1, Job("pass_1_1", "pass", ["pass"]))

        job_dir = run_files(base_dir, "r1").job_dir("pass_1_1")
        self.assertTrue((job_dir / "started.json").exists())
        self.assertFalse((job_dir / "job.json").exists())
        self.assertEqual([], stats.read_stats(base_dir))

    def test_abort_records_the_running_jobs_as_interrupted(self):
        base_dir = new_base()
        info = write_run(base_dir, "r1")
        files = run_files(base_dir, "r1")
        write_json(files.jobs, [{"id": job_id, "group": job_id[0], "args": []} for job_id in ("a_1_1", "b_1_1", "c_1_1")])
        write_json(files.job_dir("a_1_1") / "started.json", {"slot": 1, "start": time.time() - 100, "attempt": 1})
        write_json(files.job_dir("b_1_1") / "started.json", {"slot": 2, "start": time.time() - 50, "attempt": 1})
        write_json(files.job_dir("b_1_1") / "job.json", {"id": "b_1_1", "exit_code": 0})

        Orchestrator(FakePlugin(), context_for(base_dir, info)).record_interrupted()

        [record] = stats.read_stats(base_dir)
        self.assertEqual(("a_1_1", True), (record["job"], record["interrupted"]))
        self.assertAlmostEqual(100, record["end"] - record["start"], delta=2)

    def test_create_fills_the_file_from_earlier_runs(self):
        base_dir = new_base()
        info = write_run(base_dir, "r1")
        files = run_files(base_dir, "r1")
        write_json(files.state, {"phase": "done", "since": {"preparing": 100.0}})
        job_dir = files.job_dir("pass_1_1")
        write_json(job_dir / "job.json", {"id": "pass_1_1", "group": "pass", "start": 160.0, "end": 220.0, "exit_code": 0})
        write_json(job_dir / "counts.json", {"tests": 7})
        write_run(base_dir, "r2")

        self.assertEqual(["prepare", "job"], [record["type"] for record in stats.read_stats(base_dir)])
        stats.create(base_dir, "r2")
        stats.append(base_dir, stats.prepare_stats({**info, "run_id": "r2"}, 300.0, 330.0))
        stats.create(base_dir, "r2")

        records = stats.read_stats(base_dir)
        self.assertEqual([("r1", "prepare"), ("r1", "job"), ("r2", "prepare")], [(r["run_id"], r["type"]) for r in records])
        self.assertEqual((100.0, 160.0), (records[0]["start"], records[0]["end"]))
        self.assertEqual(7, records[1]["tests"])


class EstimatesTest(unittest.TestCase):
    def test_uses_the_job_then_its_group_and_skips_crashes_and_other_jdks(self):
        records = [
            job("r1", "a_1_2", "a", 100),
            job("r1", "a_2_2", "a", 300),
            job("r1", "b_1_1", "b", 1000, crashed=True),
            job("r1", "c_1_1", "c", 5000, jdk="11"),
            job("r2", "a_1_2", "a", 200),
        ]
        estimates = stats.Estimates(records, {**INFO, "run_id": "r3"})
        self.assertEqual(150, estimates.duration("a_1_2", "a"))
        self.assertEqual(200, estimates.duration("a_3_3", "a"))
        self.assertIsNone(estimates.duration("b_1_1", "b"))
        self.assertIsNone(estimates.duration("c_1_1", "c"))
        self.assertEqual([("a_1_2", "a", 1.0, False)], estimates.last_run_jobs)

    def test_an_interrupted_run_bounds_a_job_until_it_finishes_once(self):
        killed = {**job("r1", "dtest_1_2", "dtest", 929, crashed=True), "exit_code": -15}
        aborted = {**job("r2", "dtest_1_2", "dtest", 400), "crashed": None, "interrupted": True}
        records = [job("r1", "stress_1_1", "stress", 35), killed, aborted]
        estimates = stats.Estimates(records, INFO)
        self.assertEqual(929, estimates.duration("dtest_1_2", "dtest"))
        self.assertIsNone(estimates.duration("dtest_2_2", "dtest"))
        self.assertEqual([("dtest_1_2", "dtest", 1.0, False)], estimates.last_run_jobs)

        finished = stats.Estimates([*records, job("r3", "dtest_1_2", "dtest", 600)], INFO)
        self.assertEqual(600, finished.duration("dtest_1_2", "dtest"))

        killed_with_failures = {**job("r4", "dtest_2_2", "dtest", 1260), "exit_code": 137, "failures": 1}
        self.assertEqual(1260, stats.Estimates([*records, killed_with_failures], INFO).duration("dtest_2_2", "dtest"))
        self.assertIsNone(stats.Estimates([*records, killed_with_failures], INFO).duration("dtest_3_3", "dtest"))

    def test_eta_replays_the_queue_over_the_slots(self):
        estimates = stats.Estimates([job("r1", "a", "g", 100), job("r1", "b", "g", 100), job("r1", "c", "g", 50)], INFO)
        waiting = [("b", "g", 1.0, False), ("c", "g", 1.0, False)]
        self.assertEqual(110, stats.eta(estimates, waiting, [("a", "g", 1.0, 40.0)], parallel=2))
        self.assertEqual(130, stats.eta(estimates, [("a", "g", 1.0, False)], [], parallel=2, before=30.0))
        self.assertIsNone(stats.eta(stats.Estimates([], INFO), [("a", "g", 1.0, False)], [], parallel=2))

    def test_eta_spreads_divisible_work_over_all_slots(self):
        estimates = stats.Estimates([job("r1", "dt_1_2", "dt", 600), job("r1", "dt_2_2", "dt", 600)], INFO)
        running = [("dt_1_2", "dt", 0.25, 50.0)]
        waiting = [("dt_1_2", "dt", 0.75, True), ("dt_2_2", "dt", 1.0, True)]
        self.assertEqual((100 + 450 + 600) / 5, stats.eta(estimates, waiting, running, parallel=5))
        self.assertEqual(600, stats.eta(estimates, [("dt_1_2", "dt", 0.75, False), ("dt_2_2", "dt", 1.0, False)],
                                         running, parallel=5))

    def test_eta_starts_a_whole_job_once_its_memory_fits_the_budget(self):
        estimates = stats.Estimates([job("r1", f"d{index}", "d", 100) for index in range(4)] + [job("r1", "u", "u", 50)],
                                    INFO)
        waiting = [(f"d{index}", "d", 1.0, False) for index in range(1, 4)] + [("u", "u", 1.0, False)]
        memory = {**{f"d{index}": 15 for index in range(4)}, "u": 5}

        self.assertEqual(100, stats.eta(estimates, waiting, [("d0", "d", 1.0, 0.0)], parallel=8))
        self.assertEqual(200, stats.eta(estimates, waiting, [("d0", "d", 1.0, 0.0)], parallel=8, memory=memory,
                                        budget=35))

    def test_snapshot_while_preparing_counts_the_rest_of_prepare_and_the_last_run_jobs(self):
        base_dir = new_base()
        stats.append(base_dir, stats.prepare_stats({**INFO, "run_id": "r1"}, 0.0, 120.0))
        for job_id in ("pass_1_1", "fail_1_1", "crash_1_1"):
            stats.append(base_dir, job("r1", job_id, job_id.split("_")[0], 60))
        info = write_run(base_dir, "r2")
        files = run_files(base_dir, "r2")
        write_json(files.state, {"phase": "preparing", "since": {"preparing": time.time() - 20}})

        data = progress.snapshot(FakePlugin(), context_for(base_dir, info), files)

        self.assertAlmostEqual(100 + 120, data["eta"], delta=2)

    def test_splits_without_a_record_expect_the_median_tests_of_those_with_one(self):
        base_dir = new_base()
        for job_id, tests in (("dt_1_4", 900), ("dt_2_4", 1000), ("dt_3_4", 950)):
            stats.append(base_dir, job("r1", job_id, "dt", 60, tests=tests))
        info = write_run(base_dir, "r2")
        files = run_files(base_dir, "r2")
        write_json(files.jobs, [{"id": f"dt_{index}_8", "group": "dt", "args": ["dt"]} for index in range(1, 9)]
                   + [{"id": "dt_1_4", "group": "dt", "args": ["dt"]}, {"id": "dt_2_4", "group": "dt", "args": ["dt"]},
                      {"id": "dt_3_4", "group": "dt", "args": ["dt"]}])
        write_json(files.state, {"phase": "running"})

        data = progress.snapshot(FakePlugin(), context_for(base_dir, info), files)

        self.assertEqual(900 + 1000 + 950 + 8 * 950, data["groups"][0]["tests_estimate"])


if __name__ == "__main__":
    unittest.main()
