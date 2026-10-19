# SPDX-License-Identifier: Apache-2.0
"""Live details in status: containers, their usage, and the last console line of each running job."""

import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from cassandra_remote_ci.box import commands, progress
from cassandra_remote_ci.plugins.cassandra import CassandraPlugin

FULL_ID = "ab12cd34ef56" + "0" * 52


def docker(cmd):
    if cmd[1] == "ps":
        return json.dumps({"ID": FULL_ID, "Names": "jvm-dtest_1_16", "Status": "Up 3 minutes", "State": "running"}) + "\n"
    return json.dumps({"ID": FULL_ID, "CPUPerc": "312.5%", "MemUsage": "4.1GiB / 16GiB"}) + "\n"


class LastLineTest(unittest.TestCase):
    def test_skips_blank_lines_and_ansi_codes(self):
        self.assertEqual("BUILD SUCCESSFUL", progress.last_line("compile\n\x1b[32mBUILD SUCCESSFUL\x1b[0m\n\n  \n"))
        self.assertIsNone(progress.last_line("\n \n"))

    def test_cuts_long_lines(self):
        self.assertEqual(progress.LINE_WIDTH, len(progress.last_line("x" * 1000)))

    def test_reads_only_the_end_of_a_log(self):
        log = Path(tempfile.mkdtemp(prefix="cassandra-remote-ci-test-", dir="/tmp")) / "console.log"
        log.write_text("old line\n" * 100_000 + "\n[junit-timeout] Testsuite: org.apache.cassandra.FooTest\n\n")

        tail = progress.log_tail(log)

        self.assertEqual("[junit-timeout] Testsuite: org.apache.cassandra.FooTest", tail[-1])
        self.assertLess(len(tail), 100_000)
        self.assertEqual([], progress.log_tail(log.with_name("missing.log")))


class CassandraActivityTest(unittest.TestCase):
    def test_current_class_and_latest_line_without_jvm_warnings(self):
        lines = [
            "[junit-timeout] Testsuite: org.apache.cassandra.distributed.test.CASTest-_jdk17_x86_64",
            "[junit-timeout] INFO  [node1_isolatedExecutor:1] 2026-10-17 Node started",
            "[junit-timeout] WARNING: A terminally deprecated method in java.lang.System has been called",
            "[junit-timeout] WARNING: System::setSecurityManager will be removed in a future release",
            "[junit-timeout] OpenJDK 64-Bit Server VM warning: Sharing is only supported for boot loader classes",
        ]

        self.assertEqual(
            [
                "class org.apache.cassandra.distributed.test.CASTest",
                "[junit-timeout] INFO  [node1_isolatedExecutor:1] 2026-10-17 Node started",
            ],
            CassandraPlugin().activity(lines),
        )

    def test_before_the_first_class(self):
        self.assertEqual(["[exec] > Task :accord-core:jar"], CassandraPlugin().activity(["[exec] > Task :accord-core:jar"]))
        self.assertEqual(
            ["class org.apache.cassandra.FooTest"],
            CassandraPlugin().activity(["[junit-timeout] Testsuite: org.apache.cassandra.FooTest"]),
        )


class ContainerDetailsTest(unittest.TestCase):
    def test_matches_recorded_ids_to_docker_ps_and_stats(self):
        with patch.object(commands, "command_output", side_effect=docker):
            details = commands.container_details([FULL_ID, "ffffffffffff"])

        self.assertEqual(
            {FULL_ID: {"id": "ab12cd34ef56", "name": "jvm-dtest_1_16", "status": "Up 3 minutes",
                       "cpu": "312.5%", "mem": "4.1GiB / 16GiB"}},
            details,
        )

    def test_without_docker(self):
        with patch.object(commands, "command_output", return_value=""):
            self.assertEqual({}, commands.container_details([FULL_ID]))


if __name__ == "__main__":
    unittest.main()
