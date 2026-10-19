# SPDX-License-Identifier: Apache-2.0
"""One run of the orchestrator with a fake plugin."""

import io
import shutil
import signal
import subprocess
import tempfile
import unittest
from contextlib import redirect_stdout
from pathlib import Path
from unittest.mock import patch

from cassandra_remote_ci.box import summary
from cassandra_remote_ci.box.orchestrate import Orchestrator, stop_containers
from cassandra_remote_ci.shared.plugin import Job, Plugin
from cassandra_remote_ci.shared.state import context_for, read_json, run_files, write_json

PASSING = """<testsuite name="Pass"><testcase classname="Pass" name="one"/><testcase classname="Pass" name="two"/></testsuite>"""
FAILING = """<testsuite name="Fail"><testcase classname="Fail" name="one"><failure message="boom"/></testcase></testsuite>"""


class FakePlugin(Plugin):
    name = "fake"

    def __init__(self):
        self.ran: list[str] = []

    def detect(self, src):
        return False

    def default_jdk(self, src):
        return "17"

    def setup_slot(self, ctx, slot_dir):
        slot_dir.mkdir(parents=True, exist_ok=True)

    def jobs(self, ctx):
        return [Job("pass_1_1", "pass", ["pass"]), Job("fail_1_1", "fail", ["fail"]), Job("crash_1_1", "crash", ["crash"])]

    def run_job(self, ctx, slot_dir, job, job_dir, classes=None):
        self.ran.append(job.id)
        output = slot_dir / job.id
        output.mkdir(parents=True, exist_ok=True)
        if job.group == "pass":
            (output / "TEST-Pass.xml").write_text(PASSING)
            return 0
        if job.group == "fail":
            (output / "TEST-Fail.xml").write_text(FAILING)
            return 1
        (job_dir / "console.log").write_text("building\nNo space left on device\n\n")
        return 2

    def collect_results(self, ctx, slot_dir, job, job_dir):
        shutil.copytree(slot_dir / job.id, job_dir / "results", dirs_exist_ok=True)

    def report(self, ctx, crashed):
        self.reported = crashed


class OrchestrateTest(unittest.TestCase):
    def test_run_aggregates_and_resumes(self):
        base_dir = Path(tempfile.mkdtemp(prefix="cassandra-remote-ci-test-", dir="/tmp"))
        (base_dir / "src").mkdir()
        info = {
            "run_id": "r1",
            "plugin": "fake",
            "jdk": "17",
            "memory": "1g",
            "parallel": 2,
            "profile": "",
            "trash_dir": str(base_dir / "trash"),
        }
        files = run_files(base_dir, "r1")
        write_json(files.info, info)
        plugin = FakePlugin()

        with redirect_stdout(io.StringIO()):
            self.assertEqual(0, Orchestrator(plugin, context_for(base_dir, info)).run())

        self.assertEqual("done", read_json(files.state)["phase"])
        self.assertEqual(1, len(files.failures_txt.read_text().splitlines()))
        self.assertIn("crash_1_1\texit 2\tNo space left on device\t", files.crashed.read_text())
        self.assertEqual(1, len(files.crashed.read_text().splitlines()))
        self.assertIn("- crash_1_1 exit 2: `No space left on device`", files.summary_md.read_text())
        self.assertEqual(["No space left on device"], [item["cause"] for item in plugin.reported])
        snapshot = read_json(files.progress)
        self.assertEqual(3, snapshot["jobs"]["done"])
        self.assertEqual(1, snapshot["jobs"]["crashed"])
        self.assertEqual(3, sum(group["tests"] for group in snapshot["groups"]))
        self.assertEqual(1, sum(group["failures"] for group in snapshot["groups"]))
        self.assertEqual("", (base_dir / "lock").read_text())

        with redirect_stdout(io.StringIO()):
            Orchestrator(plugin, context_for(base_dir, info)).run()
        self.assertEqual(3, len(plugin.ran))


class CrashedTest(unittest.TestCase):
    def test_a_crashed_job_run_in_parts_gives_the_cause_and_log_of_the_part_that_failed(self):
        files = run_files(Path(tempfile.mkdtemp(prefix="cassandra-remote-ci-test-", dir="/tmp")), "r1")
        job_dir = files.job_dir("dt_1_1")
        for number, exit_code in ((1, 0), (2, 1)):
            write_json(job_dir / f"part-{number}/part.json", {"slot": number, "start": 0, "end": 1, "exit_code": exit_code})
            (job_dir / f"part-{number}/console.log").write_text(f"part {number} output\n")
        write_json(job_dir / "job.json", {"id": "dt_1_1", "group": "dt", "start": 0, "end": 1, "exit_code": 1, "parts": 2})

        _, crashed = summary.collect_failures(files, [Job("dt_1_1", "dt", ["dt"])], FakePlugin())

        self.assertEqual([("part 2 output", str(job_dir / "part-2/console.log"))],
                         [(item["cause"], item["log"]) for item in crashed])


class SigtermTest(unittest.TestCase):
    def test_a_second_sigterm_inside_the_handler_returns_at_once(self):
        base_dir = Path(tempfile.mkdtemp(prefix="cassandra-remote-ci-test-", dir="/tmp"))
        info = {"run_id": "r1", "plugin": "fake", "jdk": "17", "memory": "1g", "parallel": 1, "profile": "",
                "trash_dir": str(base_dir / "trash")}
        write_json(run_files(base_dir, "r1").info, info)
        orchestrator = Orchestrator(FakePlugin(), context_for(base_dir, info))
        logged = []

        def log(message):
            logged.append(message)
            if len(logged) == 1:
                orchestrator.on_sigterm(signal.SIGTERM, None)

        previous = signal.getsignal(signal.SIGTERM)
        try:
            with (
                patch("cassandra_remote_ci.box.orchestrate.log", side_effect=log),
                patch("cassandra_remote_ci.box.orchestrate.stop_containers", return_value=[]) as stop,
                patch("os._exit", side_effect=SystemExit(143)),
                self.assertRaises(SystemExit),
            ):
                orchestrator.on_sigterm(signal.SIGTERM, None)
            self.assertIs(signal.SIG_IGN, signal.getsignal(signal.SIGTERM))
        finally:
            signal.signal(signal.SIGTERM, previous)

        self.assertEqual(1, stop.call_count)
        self.assertEqual(["got SIGTERM; stopping containers", "stopped 0 containers"], logged)
        self.assertEqual("aborted", read_json(run_files(base_dir, "r1").state)["phase"])


class StopContainersTest(unittest.TestCase):
    def test_stops_running_containers_of_finished_jobs_too(self):
        files = run_files(Path(tempfile.mkdtemp(prefix="cassandra-remote-ci-test-", dir="/tmp")), "r1")
        finished = files.job_dir("fqltool-test_1_1")
        finished.mkdir(parents=True)
        (finished / "containers").write_text("aaa111\n")
        write_json(finished / "job.json", {"exit_code": -15})
        gone = files.job_dir("jvm-dtest_1_16")
        gone.mkdir(parents=True)
        (gone / "containers").write_text("bbb222\n")
        prepare = files.run_dir / "prepare"
        prepare.mkdir()
        (prepare / "containers").write_text("ccc333\n")
        calls = []

        def docker(cmd, **kwargs):
            calls.append(cmd)
            return subprocess.CompletedProcess(cmd, 0, stdout="aaa111\nccc333\n" if cmd[1] == "ps" else "")

        with patch("subprocess.run", side_effect=docker):
            stopped = stop_containers(files)

        self.assertEqual(["aaa111", "ccc333"], stopped)
        for container_id in ("aaa111", "bbb222", "ccc333"):
            self.assertIn(f"--filter=id={container_id}", calls[0])
        self.assertEqual(["docker", "stop", "-t", "10", "aaa111", "ccc333"], calls[1])


if __name__ == "__main__":
    unittest.main()
