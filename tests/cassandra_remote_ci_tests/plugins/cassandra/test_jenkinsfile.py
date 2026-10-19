# SPDX-License-Identifier: Apache-2.0
"""Cassandra jobs from the Jenkinsfile."""

import unittest
from pathlib import Path

from cassandra_remote_ci.plugins.cassandra.jenkinsfile import select_jobs

JENKINSFILE = (Path(__file__).parents[2] / "fixtures" / "cassandra" / ".jenkins" / "Jenkinsfile").read_text()


class JenkinsfileTest(unittest.TestCase):
    def test_pre_commit_on_jdk_17(self):
        jobs = select_jobs(JENKINSFILE, "pre-commit", "17", "11", with_upgrade=False)
        self.assertEqual(196, len(jobs))
        self.assertNotIn("cqlsh-test", {job.group for job in jobs})
        self.assertEqual(["test", "3/20"], next(job for job in jobs if job.id == "test_3_20").args)

    def test_pre_commit_on_jdk_11(self):
        self.assertEqual(199, len(select_jobs(JENKINSFILE, "pre-commit", "11", "11", with_upgrade=False)))

    def test_no_upgrade_jobs(self):
        jobs = select_jobs(JENKINSFILE, "pre-commit w/ upgrades", "11", "11", with_upgrade=False)
        self.assertEqual([], [job.id for job in jobs if "upgrade" in job.id])


if __name__ == "__main__":
    unittest.main()
