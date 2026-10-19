# SPDX-License-Identifier: Apache-2.0
"""Cassandra prepare: the jar build streams to prepare/console.log and records its container for abort."""

import io
import tempfile
import unittest
from contextlib import redirect_stdout
from pathlib import Path
from unittest.mock import patch

from cassandra_remote_ci.plugins.cassandra import CassandraPlugin
from cassandra_remote_ci.shared.plugin import PluginError, RunContext

BUILD_JARS = """#!/bin/bash
echo "Running container cassandra_debian-build_build-jars_jdk$1__RhbIIv 571d7716b8ce8293 using image apache/cassandra-debian-build"
echo "BUILD SUCCESSFUL"
exit "${BUILD_EXIT:-0}"
"""


def checkout() -> RunContext:
    base = Path(tempfile.mkdtemp(prefix="cassandra-remote-ci-test-", dir="/tmp"))
    (base / "cassandra-dtest").mkdir()
    (base / "cassandra-dtest/dtest.py").touch()
    script = base / "src/.build/docker/build-jars.sh"
    script.parent.mkdir(parents=True)
    script.write_text(BUILD_JARS)
    script.chmod(0o755)
    return RunContext("r1", base / "runs/r1", base / "src", "17", "16g", 5, "pre-commit", [])


class PrepareTest(unittest.TestCase):
    def test_records_the_build_container(self):
        ctx = checkout()
        out = io.StringIO()

        with redirect_stdout(out):
            CassandraPlugin().prepare(ctx)

        self.assertEqual("571d7716b8ce8293\n", (ctx.run_dir / "prepare/containers").read_text())
        self.assertIn("BUILD SUCCESSFUL", (ctx.run_dir / "prepare/console.log").read_text())
        self.assertIn("BUILD SUCCESSFUL", out.getvalue())

    def test_failed_build_raises(self):
        ctx = checkout()

        with patch.dict("os.environ", {"BUILD_EXIT": "3"}), redirect_stdout(io.StringIO()):
            with self.assertRaisesRegex(PluginError, "build-jars.sh failed with exit code 3"):
                CassandraPlugin().prepare(ctx)

        self.assertTrue((ctx.run_dir / "prepare/containers").exists())


if __name__ == "__main__":
    unittest.main()
