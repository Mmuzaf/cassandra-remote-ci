# SPDX-License-Identifier: Apache-2.0
"""Limits patch of a slot's run-tests.sh."""

import shutil
import subprocess
import tempfile
import unittest
from pathlib import Path

from cassandra_remote_ci.plugins.cassandra import jenkinsfile, limits
from cassandra_remote_ci.shared.plugin import PluginError

SCRIPT = Path(__file__).parents[2] / "fixtures" / "cassandra" / ".build" / "docker" / "run-tests.sh"
JENKINSFILE = Path(__file__).parents[2] / "fixtures" / "cassandra" / ".jenkins" / "Jenkinsfile"


class LimitsTest(unittest.TestCase):
    def setUp(self):
        self.script = Path(tempfile.mkdtemp(prefix="cassandra-remote-ci-test-", dir="/tmp")) / "run-tests.sh"
        shutil.copy(SCRIPT, self.script)

    def cpus(self, env: dict[str, str]) -> str:
        script = f"cores=16\n{self.script.read_text()}\necho $docker_cpus"
        return subprocess.run(["bash", "-c", script], env=env, capture_output=True, text=True, check=True).stdout.strip()

    def test_keeps_the_per_target_memory_and_takes_the_cpus_from_the_environment(self):
        limits.patch(self.script, "")

        text = self.script.read_text()
        self.assertIn('docker_flags="-m 5g --memory-swap 5g"', text)
        self.assertIn('docker_flags="-m 15g --memory-swap 15g"', text)
        self.assertIn("jenkins_executors=1", text)
        self.assertEqual("7", self.cpus({"docker_cpus": "7", "PATH": "/usr/bin:/bin"}))
        self.assertEqual("16.00", self.cpus({"PATH": "/usr/bin:/bin"}))

    def test_given_memory_replaces_the_per_target_memory(self):
        limits.patch(self.script, "16g")

        self.assertEqual(2, self.script.read_text().count('docker_flags="-m 16g --memory-swap 16g"'))

    def test_missing_pattern_is_named(self):
        self.script.write_text(SCRIPT.read_text().replace("| bc)", "| bc -l)"))

        with self.assertRaisesRegex(PluginError, "docker_cpus="):
            limits.patch(self.script, "")


class SizesTest(unittest.TestCase):
    def test_steps_without_a_size_are_medium(self):
        sizes = jenkinsfile.read_sizes(JENKINSFILE.read_text())

        self.assertEqual(("small", "medium", "medium", "large"),
                         (sizes["stress-test"], sizes["test"], sizes["jvm-dtest"], sizes["dtest-latest"]))


if __name__ == "__main__":
    unittest.main()
