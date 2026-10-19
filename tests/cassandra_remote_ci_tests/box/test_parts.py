# SPDX-License-Identifier: Apache-2.0
"""Jobs that run in parts: part sizes, the orchestrator handing parts to free slots, status, and the cassandra plugin."""

import io
import os
import shutil
import tempfile
import threading
import time
import unittest
from contextlib import redirect_stdout
from pathlib import Path
from unittest.mock import patch

from cassandra_remote_ci.box import progress, stats
from cassandra_remote_ci.box.orchestrate import Orchestrator, part_size
from cassandra_remote_ci.local import render
from cassandra_remote_ci.plugins.cassandra import (
    CassandraPlugin,
    crashed_html,
    split_size,
    write_cell_time,
    write_into_body,
)
from cassandra_remote_ci.shared.plugin import Job
from cassandra_remote_ci.shared.state import context_for, part_dirs, read_json, run_files, write_json
from cassandra_remote_ci_tests.box.test_orchestrate import FakePlugin

CLASSES = [f"C{index:02d}" for index in range(30)]
FIXTURES = Path(__file__).parents[1] / "fixtures" / "cassandra"


def suite(name: str, failing: bool) -> str:
    failure = '<failure message="boom"/>' if failing else ""
    return f'<testsuite name="{name}"><testcase classname="{name}" name="one">{failure}</testcase></testsuite>'


class PartsPlugin(FakePlugin):
    def __init__(self):
        super().__init__()
        self.calls: list[tuple[int, str, list[str] | None]] = []
        self.lock = threading.Lock()

    def jobs(self, ctx):
        return [Job("whole_1_1", "whole", ["whole"]), Job("dt_1_1", "dt", ["dt"])]

    def test_classes(self, ctx, job):
        return CLASSES if job.group == "dt" else []

    def run_job(self, ctx, slot_dir, job, job_dir, classes=None):
        with self.lock:
            self.calls.append((int(slot_dir.name.removeprefix("slot")), job.id, classes))
        output = slot_dir / f"{job.id}-{job_dir.name}"
        output.mkdir(parents=True, exist_ok=True)
        for name in classes or ["Whole"]:
            (output / f"TEST-{name}.xml").write_text(suite(name, failing=name == "C07"))
        time.sleep(0.05)
        return 1 if classes and "C07" in classes else 0

    def collect_results(self, ctx, slot_dir, job, job_dir):
        shutil.copytree(slot_dir / f"{job.id}-{job_dir.name}", job_dir / "results", dirs_exist_ok=True)


def new_run(parallel: int, min_classes: int):
    base_dir = Path(tempfile.mkdtemp(prefix="cassandra-remote-ci-test-", dir="/tmp"))
    (base_dir / "src").mkdir()
    info = {"run_id": "r1", "plugin": "fake", "jdk": "17", "memory": "1g", "parallel": parallel, "profile": "",
            "trash_dir": str(base_dir / "trash"), "min_classes_per_part": min_classes}
    write_json(run_files(base_dir, "r1").info, info)
    return base_dir, info


class MemoryPlugin(FakePlugin):
    """Whole jobs of 10 units of memory, and one of 50, that note how much memory runs at once."""

    def __init__(self):
        super().__init__()
        self.lock = threading.Lock()
        self.running = 0
        self.reserved: list[int] = []

    def jobs(self, ctx):
        return [Job(f"small_{index}_6", "small", ["small"]) for index in range(1, 7)] + [Job("big_1_1", "big", ["big"])]

    def job_memory(self, ctx, job):
        return 50 if job.group == "big" else 10

    def run_job(self, ctx, slot_dir, job, job_dir, classes=None):
        with self.lock:
            self.ran.append(job.id)
            self.running += self.job_memory(ctx, job)
            self.reserved.append(self.running)
        time.sleep(0.05)
        with self.lock:
            self.running -= self.job_memory(ctx, job)
        (slot_dir / job.id).mkdir(parents=True, exist_ok=True)
        return 0


class MemoryBudgetTest(unittest.TestCase):
    def test_slots_take_jobs_only_while_their_memory_fits_and_a_bigger_job_runs_alone(self):
        base_dir, info = new_run(parallel=4, min_classes=5)
        write_json(run_files(base_dir, "r1").info, {**info, "memory_budget": 25})
        plugin = MemoryPlugin()

        with redirect_stdout(io.StringIO()):
            self.assertEqual(0, Orchestrator(plugin, context_for(base_dir, info)).run())

        self.assertEqual(7, len(plugin.ran))
        self.assertEqual([20, 50], sorted(set(plugin.reserved))[-2:])
        self.assertEqual(7, len(list(run_files(base_dir, "r1").run_dir.glob("jobs/*/job.json"))))


class PartSizeTest(unittest.TestCase):
    def test_a_third_of_a_slot_share_but_never_less_than_the_minimum(self):
        self.assertEqual(14, part_size(job_left=28, all_left=56, parallel=5, minimum=10))
        self.assertEqual(6, part_size(job_left=28, all_left=56, parallel=5, minimum=5))
        self.assertEqual(16, part_size(job_left=16, all_left=44, parallel=5, minimum=10))
        self.assertEqual(5, part_size(job_left=5, all_left=5, parallel=5, minimum=10))
        self.assertEqual(28, part_size(job_left=28, all_left=441, parallel=5, minimum=10))
        self.assertEqual(225, part_size(job_left=450, all_left=3000, parallel=5, minimum=10))


class RunInPartsTest(unittest.TestCase):
    def test_free_slots_share_a_job_and_its_parts_merge_into_one_record(self):
        base_dir, info = new_run(parallel=3, min_classes=5)
        plugin = PartsPlugin()

        with redirect_stdout(io.StringIO()):
            self.assertEqual(0, Orchestrator(plugin, context_for(base_dir, info)).run())

        files = run_files(base_dir, "r1")
        parts = [call for call in plugin.calls if call[1] == "dt_1_1"]
        self.assertEqual(sorted(CLASSES), sorted(name for _, _, classes in parts for name in classes))
        self.assertGreater(len(parts), 1)
        self.assertGreater(len({slot for slot, _, _ in parts}), 1)
        self.assertTrue(all(len(classes) >= 5 for _, _, classes in parts))

        job_dir = files.job_dir("dt_1_1")
        record = read_json(job_dir / "job.json")
        self.assertEqual((len(parts), 1), (record["parts"], record["exit_code"]))
        self.assertEqual(len(parts), len(part_dirs(job_dir)))
        self.assertEqual(30, read_json(job_dir / "counts.json")["tests"])
        self.assertEqual(30, len(list((job_dir / "results").glob("TEST-*.xml"))))
        self.assertEqual(["C07"], [failure["classname"] for failure in read_json(job_dir / "failures.json")])
        self.assertIn("C07.one", files.failures_txt.read_text())

        [dt] = [record for record in stats.read_stats(base_dir) if record.get("job") == "dt_1_1"]
        self.assertTrue(dt["divisible"])
        self.assertGreaterEqual(dt["work"], dt["end"] - dt["start"])

    def test_status_counts_finished_parts_and_shows_running_ones(self):
        base_dir, info = new_run(parallel=3, min_classes=5)
        files = run_files(base_dir, "r1")
        write_json(files.jobs, [{"id": "dt_1_1", "group": "dt", "args": ["dt"]}])
        write_json(files.state, {"phase": "running", "since": {"running": time.time() - 100}})
        job_dir = files.job_dir("dt_1_1")
        write_json(job_dir / "part-1" / "started.json", {"slot": 1, "start": time.time() - 90, "classes": CLASSES[:10]})
        write_json(job_dir / "part-1" / "part.json", {"slot": 1, "start": time.time() - 90, "end": time.time() - 30,
                                                      "exit_code": 0})
        write_json(job_dir / "part-1" / "counts.json", {"tests": 10, "classes": 10})
        write_json(job_dir / "part-2" / "started.json", {"slot": 2, "start": time.time() - 60, "classes": CLASSES[10:17]})
        stats.append(base_dir, {**stats.job_stats(info, {"id": "dt_1_1", "group": "dt", "start": 0.0, "end": 300.0,
                                                         "exit_code": 0}, 30, 0, 300.0), "run_id": "r0"})

        data = progress.snapshot(PartsPlugin(), context_for(base_dir, info), files)

        [running] = data["jobs"]["running"]
        self.assertEqual((2, 2, 7), (running["slot"], running["part"], running["classes"]))
        self.assertAlmostEqual(70, running["expected"])
        self.assertEqual(10, data["groups"][0]["tests"])
        self.assertAlmostEqual((10 + 130) / 3, data["eta"], delta=1)


    def test_eta_while_preparing_spreads_the_planned_divisible_jobs(self):
        base_dir, info = new_run(parallel=3, min_classes=5)
        files = run_files(base_dir, "r1")
        write_json(files.state, {"phase": "preparing", "since": {"preparing": time.time() - 20}})
        old = {**info, "run_id": "r0"}
        stats.append(base_dir, stats.prepare_stats(old, 0.0, 100.0))
        for job_id, group, work in (("whole_1_1", "whole", 60.0), ("dt_1_1", "dt", 600.0)):
            record = {"id": job_id, "group": group, "start": 0.0, "end": work, "exit_code": 0}
            stats.append(base_dir, stats.job_stats(old, record, 1, 0, work))

        data = progress.snapshot(PartsPlugin(), context_for(base_dir, info), files)

        self.assertAlmostEqual(80 + (60 + 600) / 3, data["eta"], delta=2)
        self.assertFalse([line for line in render.render(data).splitlines() if line.startswith("jobs ")])


class CassandraPartsTest(unittest.TestCase):
    def test_split_classes_and_the_command_of_a_part(self):
        src = Path(tempfile.mkdtemp(prefix="cassandra-remote-ci-test-", dir="/tmp"))
        root = src / "test/distributed/org/apache/cassandra/distributed"
        for name in ("test/ATest", "test/BTest", "test/CTest", "test/DTest", "upgrade/EUpgradeTest", "test/FTest"):
            (root / f"{name}.java").parent.mkdir(parents=True, exist_ok=True)
            (root / f"{name}.java").write_text("")
        (src / ".jenkins").mkdir()
        shutil.copy(FIXTURES / ".jenkins/Jenkinsfile", src / ".jenkins/Jenkinsfile")
        ctx = context_for(src.parent, {"run_id": "r1", "jdk": "17", "memory": "", "parallel": 2, "profile": "",
                                       "trash_dir": "/tmp/unused"})
        ctx = ctx.__class__(**{**ctx.__dict__, "src": src})
        plugin = CassandraPlugin()

        prefix = "org/apache/cassandra/distributed/test/"
        second = plugin.test_classes(ctx, Job("jvm-dtest_2_2", "jvm-dtest", ["jvm-dtest", "2/2"]))
        self.assertEqual([prefix + "BTest.java", prefix + "DTest.java"], second)
        self.assertEqual([], plugin.test_classes(ctx, Job("dtest_1_2", "dtest", ["dtest", "1/2"])))

        jobs = [Job("jvm-dtest_2_2", "jvm-dtest", ["jvm-dtest", "2/2"]), Job("dtest_1_2", "dtest", ["dtest", "1/2"])]
        with patch.object(CassandraPlugin, "run_logged", return_value=0) as run_logged:
            plugin.run_job(ctx, src.parent / "slot1", Job("jvm-dtest_2_2", "jvm-dtest", ["jvm-dtest", "2/2"]),
                           src.parent / "part-1", second)
            plugin.run_job(ctx, src.parent / "slot1", Job("dtest_1_2", "dtest", ["dtest", "1/2"]), src.parent / "job")
        (part, _, part_env, *_), (whole, _, whole_env, *_) = (call.args for call in run_logged.call_args_list)
        self.assertEqual(["-a", "jvm-dtest", "-t", ",".join(second), "-j", "17"], part[1:])
        self.assertEqual(["-a", "dtest", "-c", "1/2", "-j", "17"], whole[1:])
        self.assertEqual(("3", "7"), (part_env["docker_cpus"], whole_env["docker_cpus"]))
        self.assertEqual((5 * 2**30, 15 * 2**30), (plugin.job_memory(ctx, jobs[0]), plugin.job_memory(ctx, jobs[1])))

        self.assertEqual({"jvm-dtest": 2}, plugin.totals(ctx, jobs).classes_per_group)

    def test_dtest_total_counts_the_tests_of_the_runs_splits(self):
        self.assertEqual(18, split_size(1105, "1/64"))
        self.assertEqual(17, split_size(1105, "18/64"))
        self.assertEqual(1105, sum(split_size(1105, f"{index}/64") for index in range(1, 65)))
        self.assertEqual(1105, split_size(1105, "test_cqlsh"))

        base_dir, info = new_run(parallel=2, min_classes=5)
        files = run_files(base_dir, "r1")
        write_json(files.jobs, [{"id": f"dtest_{index}_64", "group": "dtest", "args": ["dtest", f"{index}/64"]}
                                for index in (1, 18)])
        test_list = base_dir / "test_list.txt"
        test_list.write_text("".join(f"test_{index}\n" for index in range(1105)))

        CassandraPlugin().record_test_total(context_for(base_dir, info), "dtest", test_list)

        self.assertEqual({"dtest": 35}, read_json(files.totals)["tests_per_group"])

    def test_results_left_in_the_slot_by_an_earlier_job_are_not_in_progress(self):
        slot_dir = Path(tempfile.mkdtemp(prefix="cassandra-remote-ci-test-", dir="/tmp"))
        output = slot_dir / "src/build/test/output/test"
        output.mkdir(parents=True)
        (output / "TEST-Old.xml").write_text(suite("Old", False))
        os.utime(output / "TEST-Old.xml", (1000.0, 1000.0))
        (output / "TEST-New.xml").write_text(suite("New", False))

        self.assertEqual([output / "TEST-New.xml"], CassandraPlugin().in_progress_results(slot_dir, 2000.0))

    def test_cell_time_of_a_job_run_in_parts_sums_its_parts(self):
        job_dir = Path(tempfile.mkdtemp(prefix="cassandra-remote-ci-test-", dir="/tmp")) / "jvm-dtest_2_16"
        for number, (start, end) in enumerate([(100.0, 700.0), (100.0, 1600.0)], 1):
            write_json(job_dir / f"part-{number}/part.json", {"slot": number, "start": start, "end": end, "exit_code": 0})
        xml = job_dir / "results/output/TEST-a.xml"
        xml.parent.mkdir(parents=True)
        xml.write_text('<testsuite name="A"><testcase classname="A" name="x" time="30.5"/>'
                       '<testcase classname="B" name="y" time="90"/><testcase classname="A" name="z" time="9.5"/>'
                       '</testsuite>')
        record = {"id": "jvm-dtest_2_16", "group": "jvm-dtest", "start": 100.0, "end": 1600.0, "exit_code": 0}

        with patch("platform.machine", return_value="x86_64"):
            write_cell_time(job_dir.parent / "cell-times", job_dir, record, "17")

        cell_times = job_dir.parent / "cell-times"
        self.assertEqual("jvm-dtest\tamd64\t17\t2\t16\t3\t2100000\t130.000\tok\n",
                         (cell_times / "jvm-dtest_2_16.tsv").read_text())
        self.assertEqual("90.000\tB\n40.000\tA\n", (cell_times / "jvm-dtest_2_16.suites").read_text())


class CassandraCrashTest(unittest.TestCase):
    def test_the_cause_is_the_first_error_after_the_last_log_line_before_build_failed(self):
        lines = [
            "     [java] ERROR [main] 2026-10-18T08:41:55,001 Logged.java:1 - java.lang.IllegalStateException: logged",
            "     [java] INFO  [main] 2026-10-18T08:41:56,166 RegistrationStatus.java:51 - Node is initializing",
            "     [java] <failure>",
            "     [java] java.io.UTFDataFormatException: malformed input around byte 95",
            "     [java] \tat java.base/java.io.DataInputStream.readUTF(DataInputStream.java:649)",
            "     [java] ERROR: org.openjdk.jmh.runner.RunnerException: Benchmark caught the exception",
            "BUILD FAILED",
            "json.decoder.JSONDecodeError: Expecting value: line 1 column 1 (char 0)",
            "+ exit 1",
        ]

        self.assertEqual("java.io.UTFDataFormatException: malformed input around byte 95",
                         CassandraPlugin().crash_cause(lines))

    def test_a_pip_error_is_a_cause_and_without_an_error_the_last_line_that_is_not_a_shell_trace(self):
        self.assertEqual("ERROR: Cannot install ccm because these package versions have conflicting dependencies.",
                         CassandraPlugin().crash_cause(["Collecting ccm", "ERROR: Cannot install ccm because these"
                                                        " package versions have conflicting dependencies.", "+ exit 1"]))
        self.assertEqual("failed dtest 1/64", CassandraPlugin().crash_cause(["failed dtest 1/64", "+ exit 1", ""]))

    def test_ci_summary_lists_the_crashed_jobs_inside_its_body(self):
        run_dir = Path(tempfile.mkdtemp(prefix="cassandra-remote-ci-test-", dir="/tmp"))
        html = run_dir / "ci_summary.html"
        html.write_text("<html><body><h1>CI Summary</h1></body>\n</html>\n")
        crashed = [{"job": "microbench-test_4_4", "group": "microbench-test", "exit_code": 1, "cause": "<failure>",
                    "log": str(run_dir / "jobs/microbench-test_4_4/console.log")}]

        write_into_body(html, crashed_html(crashed, run_dir))

        document = html.read_text()
        self.assertIn("<td><code>&lt;failure&gt;</code></td>", document)
        self.assertIn("<a href='../jobs/microbench-test_4_4/console.log'>", document)
        self.assertTrue(document.endswith("</table></body>\n</html>\n"))


if __name__ == "__main__":
    unittest.main()
