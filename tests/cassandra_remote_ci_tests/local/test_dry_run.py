# SPDX-License-Identifier: Apache-2.0
"""--dry-run prints commands and runs none."""

import io
import re
import unittest
from contextlib import redirect_stderr, redirect_stdout
from pathlib import Path
from unittest.mock import patch

from cassandra_remote_ci.local.box import UsageError
from cassandra_remote_ci.local.cli import git_ignored, main, memory_budget
from cassandra_remote_ci.local.ssh import CommandRunner, Result
from cassandra_remote_ci.plugins.cassandra import SLOT_EXCLUDES, CassandraPlugin
from cassandra_remote_ci.shared.plugin import Job, PluginError, RunContext, parse_size

CASSANDRA = str(Path(__file__).parents[1] / "fixtures" / "cassandra")
DTEST = str(Path(__file__).parents[1] / "fixtures" / "dtest")


def run_cli(*argv: str) -> tuple[int, str, str]:
    out, err = io.StringIO(), io.StringIO()
    forbidden = AssertionError("dry run started a process")
    with (
        patch("subprocess.run", side_effect=forbidden),
        patch("subprocess.Popen", side_effect=forbidden),
        redirect_stdout(out),
        redirect_stderr(err),
    ):
        code = main(list(argv))
    return code, out.getvalue(), err.getvalue()


class DryRunTest(unittest.TestCase):
    def test_run_prints_sync_and_unit(self):
        code, out, err = run_cli("run", "-H", "ubuntu", "-C", CASSANDRA, "-j", "17", "--dry-run")

        self.assertEqual(0, code, err)
        self.assertIn("--exclude /build/", out)
        self.assertIn("ControlMaster=auto", out)
        self.assertIn("systemd-run --user --unit cassandra-remote-ci-", out)
        self.assertIn("-p TimeoutStopSec=30s -p KillMode=mixed", out)
        self.assertIn("jobs     196 total", out)
        self.assertRegex(out, r"\n  test +20 +5\.0G\n")
        self.assertRegex(out, r"\n  dtest +64 +15\.0G\n")

    def test_run_syncs_given_dtest_dir(self):
        code, out, err = run_cli(
            "run", "-H", "ubuntu", "-C", CASSANDRA, "-j", "17", "--dry-run", "--", "--with-upgrade", "--", "-d", DTEST
        )

        self.assertEqual(0, code, err)
        self.assertIn(f"{DTEST}/ ubuntu:REMOTE_HOME/cassandra-remote-ci/cassandra-dtest/", out)

    def test_abort_prints_systemctl_stop(self):
        code, out, err = run_cli("abort", "-H", "ubuntu", "--dry-run")

        self.assertEqual(0, code, err)
        self.assertIn("systemctl --user stop cassandra-remote-ci-RUN_ID", out)

    def test_logs_follow_events_unless_all(self):
        code, out, err = run_cli("logs", "-H", "ubuntu", "--dry-run")
        self.assertEqual(0, code, err)
        self.assertIn("-n 200 -f -o cat --grep ", out)
        self.assertIn("^[0-9]{2}:[0-9]{2}:[0-9]{2} ", out)

        code, out, err = run_cli("logs", "-H", "ubuntu", "--dry-run", "--all")
        self.assertEqual(0, code, err)
        self.assertIn("journalctl --user -u cassandra-remote-ci-RUN_ID -n 200 -f", out)
        self.assertNotIn("--grep", out)

    def test_opensearch_is_not_implemented(self):
        code, out, err = run_cli("-P", "opensearch", "run", "-H", "ubuntu", "-C", CASSANDRA, "--dry-run")

        self.assertNotEqual(0, code)
        self.assertIn("not implemented", err)
        self.assertNotIn("would run", out)

    def test_cleanup_moves_into_trash(self):
        code, out, err = run_cli("cleanup", "-H", "ubuntu", "--older-than", "7d", "--dry-run")

        self.assertEqual(0, code, err)
        commands = [line for line in out.splitlines() if line.startswith("would run:")]
        self.assertTrue(any(re.search(r"\bmv .* /tmp/cassandra-remote-ci-trash/", line) for line in commands))
        self.assertFalse(any(re.search(r"\brm ", line) for line in commands))
        self.assertIn("rm -rf /tmp/cassandra-remote-ci-trash/", out)


class FakeGit(CommandRunner):
    """Answers git ls-files and git config for /checkout with one submodule, modules/accord."""

    def run(self, cmd, *, input=None, capture=False, check=True):
        path = cmd[2]
        if "ls-files" in cmd:
            return Result(0, {"/checkout": "data/\0lib/x.jar\0", "/checkout/modules/accord": "build/\0"}[path])
        if path == "/checkout":
            return Result(0, "submodule.modules/accord.path\nmodules/accord\0")
        return Result(1, "")


class GitIgnoredTest(unittest.TestCase):
    def test_ignored_paths_of_checkout_and_submodules_become_anchored_excludes(self):
        with patch.object(Path, "exists", return_value=True):
            excludes = git_ignored(FakeGit(), Path("/checkout"))

        self.assertEqual(["/data/", "/lib/x.jar", "/modules/accord/build/"], excludes)

    def test_not_a_git_checkout(self):
        self.assertEqual([], git_ignored(FakeGit(), Path(CASSANDRA)))


class DockerMemoryTest(unittest.TestCase):
    @staticmethod
    def remote(memory: int):
        return type("FakeRemote", (), {"host": "ubuntu", "docker_memory": memory, "docker_os": "Docker Desktop"})()

    def test_sizes(self):
        self.assertEqual(16 * 2**30, parse_size("16g"))
        self.assertEqual(512 * 2**20, parse_size("512M"))
        with self.assertRaises(PluginError):
            parse_size("lots")

    def test_the_budget_is_docker_memory_less_the_reserve_and_must_fit_the_largest_job(self):
        ctx = RunContext("r1", Path("/runs/r1"), Path(CASSANDRA), "17", "", 8, "pre-commit", [])
        plugin = CassandraPlugin()
        jobs = [Job("test_1_20", "test", ["test", "1/20"]), Job("dtest_1_64", "dtest", ["dtest", "1/64"])]

        with self.assertRaisesRegex(UsageError, r"Docker Desktop\) has 16.0G of memory, dtest_1_64 needs 15.0G "
                                                r"and the box keeps 8.0G"):
            memory_budget(self.remote(16 * 2**30), plugin, ctx, jobs)
        self.assertEqual(88 * 2**30, memory_budget(self.remote(96 * 2**30), plugin, ctx, jobs))
        self.assertIsNone(memory_budget(self.remote(0), plugin, ctx, jobs))


class SlotExcludesTest(unittest.TestCase):
    def test_test_classes_reach_the_slots(self):
        self.assertFalse(any("/build/test/classes/".startswith(pattern) for pattern in SLOT_EXCLUDES))


if __name__ == "__main__":
    unittest.main()
