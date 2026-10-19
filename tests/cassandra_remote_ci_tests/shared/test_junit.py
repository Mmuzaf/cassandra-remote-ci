# SPDX-License-Identifier: Apache-2.0
"""JUnit XML parsing."""

import tempfile
import unittest
from pathlib import Path

from cassandra_remote_ci.shared import junit

FIXTURES = Path(__file__).parents[1] / "fixtures" / "junit"


class JunitTest(unittest.TestCase):
    def test_counts_and_failures(self):
        results = junit.parse_results(junit.find_xml(FIXTURES))

        counts = junit.count(results)
        self.assertEqual((4, 1, 1, 1, 1), (counts.tests, counts.failures, counts.errors, counts.skipped, counts.classes))
        failing = junit.failing(results)
        self.assertEqual(["org.example.MixedTest.fails", "org.example.MixedTest.errors"], [r.test for r in failing])
        self.assertEqual("expected 1 but was 2", failing[0].message)

    def test_a_class_is_the_suite_ant_ran_not_the_classes_its_cases_name(self):
        directory = Path(tempfile.mkdtemp(prefix="cassandra-remote-ci-test-", dir="/tmp"))
        (directory / "TEST-org.example.OuterTest.xml").write_text(
            '<testsuite name="org.example.OuterTest"><testcase classname="org.example.OuterTest$Inner" name="a"/>'
            '<testcase classname="com.google.common.collect.testing.testers.CollectionAddTester" name="b"/>'
            '<testcase classname="junit.framework.JUnit4TestCaseFacade" name="c"/></testsuite>')
        (directory / "nosetests.xml").write_text(
            '<testsuites><testsuite name="pytest"><testcase classname="cql_test.TestCQL" name="d"/>'
            '<testcase classname="scrub_test.TestScrub" name="e"/></testsuite></testsuites>')

        self.assertEqual(3, junit.count(junit.parse_results(junit.find_xml(directory))).classes)


if __name__ == "__main__":
    unittest.main()
