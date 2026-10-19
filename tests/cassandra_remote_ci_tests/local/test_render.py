# SPDX-License-Identifier: Apache-2.0
"""The status text the Mac renders from the box's JSON."""

import io
import json
import unittest
from contextlib import redirect_stdout
from types import SimpleNamespace

from cassandra_remote_ci.local import render
from cassandra_remote_ci.local.cli import show_status
from cassandra_remote_ci.local.ssh import Result


def group(name, jobs, jobs_total, classes, classes_total, tests, tests_estimate, failures=0, skipped=0):
    return {"group": name, "jobs": jobs, "jobs_total": jobs_total, "classes": classes, "classes_total": classes_total,
            "tests": tests, "tests_total": None, "tests_estimate": tests_estimate, "failures": failures, "errors": 0,
            "skipped": skipped}


class RenderTest(unittest.TestCase):
    def test_tables_of_running_jobs_and_groups(self):
        data = {
            "run_id": "r1",
            "phase": "running",
            "elapsed": 660,
            "eta": 420,
            "log": "19:40:07 end fqltool-test_1_1 exit 0 in 55s, 27 tests, 0 failures, 0 errors",
            "jobs": {
                "done": 2,
                "total": 4,
                "crashed": 0,
                "running": [
                    {
                        "job": "jvm-dtest_2_16",
                        "slot": 3,
                        "elapsed": 600,
                        "expected": 500,
                        "containers": [],
                        "activity": ["class org.apache.cassandra.distributed.test.ReadSpeculationTest"],
                    },
                    {
                        "job": "jvm-dtest_1_16",
                        "slot": 2,
                        "part": 3,
                        "classes": 12,
                        "elapsed": 600,
                        "expected": 1020,
                        "containers": [{"id": "ab12cd34ef56", "status": "Up 10 minutes", "cpu": "4.06%",
                                        "mem": "2.128GiB / 16GiB"}],
                        "activity": ["class org.apache.cassandra.FooTest", "[junit-timeout] Node started"],
                    },
                ],
            },
            "groups": [
                group("stress-test", 1, 1, 8, None, 22, 22),
                group("jvm-dtest", 0, 2, 27, 56, 187, 1200, failures=1, skipped=4),
            ],
            "recent": [{"group": "jvm-dtest", "test": "FooTest.bar", "status": "failure", "job": "jvm-dtest_1_16"}],
        }

        self.assertEqual(
            """\
r1: running, elapsed 11m, eta ~7m, 2 of 4 jobs done, 2 running, unit active
---------------------------------------------------------------------------

SLOT  JOB             PART  CLASSES  ELAPSED  LEFT  CONTAINER     STATE            CPU               MEM
====  ==============  ====  =======  =======  ====  ============  =============  =====  ================
   2  jvm-dtest_1_16     3       12      10m   ~7m  ab12cd34ef56  Up 10 minutes  4.06%  2.128GiB / 16GiB
   3  jvm-dtest_2_16     -        -      10m   +1m  -             -                  -                 -

GROUP        JOBS  CLASSES       TESTS  FAIL  ERR  SKIP
===========  ====  =======  ==========  ====  ===  ====
stress-test   1/1        8      22/~22     0    0     0
jvm-dtest     0/2    27/56  187/~1,200     1    0     4
-----------  ----  -------  ----------  ----  ---  ----
total         2/4    27/56  209/~1,222     1    0     4

RECENT_FAILURE  STATUS   GROUP      JOB
==============  =======  =========  ==============
FooTest.bar     failure  jvm-dtest  jvm-dtest_1_16

Activity
  slot2  class org.apache.cassandra.FooTest
         [junit-timeout] Node started
  slot3  class org.apache.cassandra.distributed.test.ReadSpeculationTest

Log
  19:40:07 end fqltool-test_1_1 exit 0 in 55s, 27 tests, 0 failures, 0 errors""",
            render.render(data, "active"),
        )

    def test_a_run_whose_unit_stopped_before_it_finished_is_interrupted(self):
        data = {
            "run_id": "r1",
            "phase": "running",
            "elapsed": 24540,
            "eta": 20460,
            "usage": {"cpu": 0.73, "cpu_average": 0.68, "memory_used": 21 * 2**30, "memory_total": 90 * 2**30},
            "memory_budget": 82 * 2**30,
            "memory_reserved": 75 * 2**30,
            "log": "00:36:57 start dtest_18_64 in slot4",
            "jobs": {"done": 77, "total": 196, "crashed": 0, "running": [
                {"job": "dtest_14_64", "slot": 1, "elapsed": 10020, "expected": 1500, "containers": [], "activity": []},
            ]},
            "groups": [group("dtest", 13, 64, 13, None, 234, 1106)],
            "recent": [],
        }

        lines = render.render(data, "inactive").splitlines()

        self.assertEqual("r1: interrupted, elapsed 6h49m, 77 of 196 jobs done, 1 interrupted, unit inactive", lines[0])
        self.assertFalse([line for line in lines if line.startswith("box:")])
        self.assertIn("  the run's unit stopped before the run finished, e.g. on a reboot; "
                      "'retry' runs the interrupted and remaining jobs", lines)
        self.assertIn("running, elapsed", render.render(data, "active").splitlines()[0])

    def test_box_usage_line(self):
        data = {"usage": {"cpu": 0.93, "cpu_average": 0.81, "memory_used": 61 * 2**30, "memory_total": 94 * 2**30},
                "memory_budget": 86 * 2**30, "memory_reserved": 75 * 2**30}
        self.assertEqual("box: CPU 93% busy, 81% on average, memory 61.0G of 94.0G used, "
                         "jobs reserve 75.0G of the 86.0G budget", render.box_line(data))
        self.assertEqual("", render.box_line({}))


OLD_BOX_SNAPSHOT = {
    "run_id": "20261017-1955-cassandra-5182b2b-jdk17",
    "phase": "running",
    "elapsed": 1140,
    "eta": None,
    "unit": "active",
    "log": "19:57:13 end fqltool-test_1_1 exit 0 in 50s, 27 tests, 0 failures, 0 errors",
    "jobs": {"done": 2, "total": 4, "crashed": 0, "running": [
        {"job": "jvm-dtest_1_16", "slot": 2, "elapsed": 1080,
         "containers": [{"id": "ab12cd34ef56", "name": "cassandra_ubuntu-test_jvm-dtest_1_16__FgruQo",
                         "status": "Up 18 minutes", "cpu": "3.06%", "mem": "2.634GiB / 16GiB"}],
         "activity": ["class org.apache.cassandra.distributed.test.accord.InteropAccordCQLTest"]},
    ]},
    "groups": [{"group": "jvm-dtest", "jobs": 0, "jobs_total": 2, "classes": 34, "classes_total": 441, "tests": 231,
                "tests_total": None, "tests_estimate": None, "failures": 0, "errors": 0, "skipped": 5,
                "mean_duration": None}],
    "recent": [],
}


class ShowStatusTest(unittest.TestCase):
    def test_renders_the_box_json_here_even_from_an_older_package(self):
        calls = []

        def python(args, **options):
            calls.append(args)
            return Result(0, json.dumps(OLD_BOX_SNAPSHOT))

        remote = SimpleNamespace(runner=SimpleNamespace(dry=False), python=python)
        out = io.StringIO()
        with redirect_stdout(out):
            self.assertEqual(0, show_status(remote, "r1", as_json=False))

        self.assertEqual([["status", "r1", "--json"]], calls)
        text = out.getvalue()
        self.assertTrue(text.startswith(
            "20261017-1955-cassandra-5182b2b-jdk17: running, elapsed 19m, 2 of 4 jobs done, 1 running, unit active\n"
        ))
        self.assertIn("   2  jvm-dtest_1_16     -        -      18m     -  ab12cd34ef56  Up 18 minutes  3.06%", text)
        self.assertIn("jvm-dtest   0/2   34/441    231     0    0     5", text)


if __name__ == "__main__":
    unittest.main()
