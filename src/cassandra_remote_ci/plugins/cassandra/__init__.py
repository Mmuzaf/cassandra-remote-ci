# SPDX-License-Identifier: Apache-2.0
"""Cassandra plugin that runs Jenkinsfile pipeline profiles through .build/docker/run-tests.sh in parallel slots."""

from __future__ import annotations

import argparse
import os
import platform
import re
import shutil
import subprocess
import threading
from collections import Counter
from html import escape
from pathlib import Path

from cassandra_remote_ci.plugins.cassandra import jenkinsfile, limits
from cassandra_remote_ci.shared import junit
from cassandra_remote_ci.shared.plugin import Job, Plugin, PluginError, RunContext, Totals, parse_size, sync_tree
from cassandra_remote_ci.shared.state import RunFiles, job_record, job_start, part_dirs, read_json, write_json

DTEST_REPOSITORY = "https://github.com/apache/cassandra-dtest.git"
CONTAINER_LINE = re.compile(r"^Running container \S+ ([0-9a-f]{12,})")
TESTSUITE_LINE = re.compile(r"Testsuite: ([\w.$]+)")
JVM_WARNING = re.compile(r"(^|\] )WARNING: | VM warning: ")
ANT_TASK = re.compile(r"^\s*\[\w+\] ?")
LOG_LINE = re.compile(r"^(TRACE|DEBUG|INFO|WARN|ERROR) +\[")
ERROR_LINE = re.compile(r"^((Caused by|Suppressed): )?([\w$]+\.)+[\w$]*(Exception|Error)\b|^ERROR\b|\berror: ")
DOCKER_TIMEOUT_HOURS = 3
# CPUs of each Jenkins agent size, and the memory limit run-tests.sh gives the targets of that size.
SIZES = {"small": (1, "5g"), "medium": (3, "5g"), "large": (7, "15g")}
UNIT_GROUPS = ("test", "test-latest", "test-cdc", "test-compression", "test-oa", "test-system-keyspace-directory")
CLASS_DIRS = {"test-burn": "burn", "long-test": "long", "simulator-dtest": "simulator", "jvm-dtest": "distributed"}
# Per-job outputs in a slot; run-tests.sh clears them itself. build/test/classes from prepare must reach the slots.
SLOT_EXCLUDES = (
    "/build/test/output/",
    "/build/test/logs/",
    "/build/test/reports/",
    "/build/test/html/",
    "/build/test/cassandra/",
    "/build/tmp/",
)


def split_size(listed: int, split: str) -> int:
    """Return how many of listed lines split -n r/K/N deals to split K/N, or all of them for another selection."""
    match = re.fullmatch(r"(\d+)/(\d+)", split)
    if not match:
        return listed
    index, count = map(int, match.groups())
    return listed // count + (1 if index <= listed % count else 0)


def write_cell_time(cell_times: Path, job_dir: Path, record: dict, jdk: str) -> None:
    """Write the <job>.tsv and <job>.suites that writeCellTime in the Jenkinsfile writes for a cell.

    .build/ci/cell_balance.py reads them for the split balance table of ci_summary.html. A job run in parts
    takes as long as its parts together, the time it would take as one Jenkins cell.
    """
    step, index, count = record["id"].rsplit("_", 2)
    parts = [read_json(part_dir / "part.json") for part_dir in part_dirs(job_dir)]
    spans = [(part["start"], part["end"]) for part in parts if part] or [(record["start"], record["end"])]
    seconds = junit.class_seconds(junit.find_xml(job_dir / "results/output"))
    longest = sorted(seconds.items(), key=lambda item: -item[1])[:12]
    arch = {"x86_64": "amd64", "aarch64": "arm64"}.get(platform.machine(), platform.machine())
    fields = [step, arch, jdk, index, count, str(DOCKER_TIMEOUT_HOURS),
              str(round(sum(end - start for start, end in spans) * 1000)), f"{sum(seconds.values()):.3f}",
              "ok" if record["exit_code"] == 0 else "failed"]
    cell_times.mkdir(parents=True, exist_ok=True)
    (cell_times / f"{record['id']}.tsv").write_text("\t".join(fields) + "\n")
    (cell_times / f"{record['id']}.suites").write_text("".join(f"{time:.3f}\t{name}\n" for name, time in longest))


def crashed_html(crashed: list[dict], run_dir: Path) -> str:
    """Return a ci_summary.html section of the crashed jobs, with links to their logs from the report directory."""
    rows = []
    for item in crashed:
        log = os.path.relpath(item["log"], run_dir)
        rows.append(f"<tr><td>{escape(item['job'])}</td><td>{item['exit_code']}</td>"
                    f"<td><code>{escape(item.get('cause', ''))}</code></td>"
                    f"<td><a href='../{escape(log)}'>{escape(log)}</a></td></tr>")
    return ("<h2>Crashed jobs</h2><p>Jobs that exited non-zero without a failing test. The cause is the line"
            " of the console log that reports the error.</p><table border='1' cellpadding='4'><tr><th>job</th>"
            "<th>exit</th><th>cause</th><th>log</th></tr>" + "".join(rows) + "</table>")


def write_into_body(path: Path, html: str) -> None:
    """Insert html before the last </body>, or append it to a document without one."""
    head, tag, tail = path.read_text(encoding="utf-8").rpartition("</body>")
    path.write_text(head + html + tag + tail if tag else tail + html, encoding="utf-8")


def _copy_newer(source: Path, target: Path, since: float) -> None:
    """Copy the files under source changed at or after since, so leftovers of earlier jobs stay behind."""
    if not source.is_dir():
        return
    for path in source.rglob("*"):
        if path.is_file() and path.stat().st_mtime >= since - 1:
            destination = target / path.relative_to(source)
            destination.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(path, destination)


class CassandraPlugin(Plugin):
    name = "cassandra"
    default_parallel = 8
    default_memory = ""
    default_profile = "pre-commit"
    sync_excludes = ["/build/", ".idea/"]
    remote_commands = ["bc"]

    def __init__(self):
        self.totals_lock = threading.Lock()
        self.class_cache: dict[str, list[str]] = {}
        self.size_cache: dict[str, str] = {}

    @staticmethod
    def options(plugin_args: list[str]) -> argparse.Namespace:
        parser = argparse.ArgumentParser(prog="cassandra plugin", add_help=False)
        parser.add_argument("--with-upgrade", action="store_true")
        parser.add_argument("-d", "--dtest-dir", type=Path)
        options, unknown = parser.parse_known_args(plugin_args)
        if unknown:
            raise PluginError(f"the cassandra plugin doesn't accept {' '.join(unknown)}; use --with-upgrade or -d <dir>")
        return options

    def detect(self, src: Path) -> bool:
        build_xml = src / "build.xml"
        return (
            build_xml.is_file()
            and 'name="apache-cassandra"' in build_xml.read_text(errors="replace")
            and (src / ".build/docker/run-tests.sh").is_file()
        )

    def default_jdk(self, src: Path) -> str:
        return jenkinsfile.build_property(src / "build.xml", "java.default")

    def extra_sources(self, src: Path, plugin_args: list[str]) -> dict[str, Path]:
        given = self.options(plugin_args).dtest_dir
        dtest = (given or src.parent / "cassandra-dtest").expanduser().resolve()
        if (dtest / "dtest.py").is_file():
            return {"cassandra-dtest": dtest}
        if given:
            raise PluginError(f"{dtest} has no dtest.py")
        return {}

    def prepare(self, ctx: RunContext) -> None:
        dtest = ctx.src.parent / "cassandra-dtest"
        if not (dtest / "dtest.py").is_file():
            subprocess.run(["git", "clone", "--depth", "1", DTEST_REPOSITORY, str(dtest)], check=True)
        prepare_dir = ctx.run_dir / "prepare"

        def on_line(line: str) -> None:
            print(line, flush=True)
            match = CONTAINER_LINE.match(line)
            if match:
                self.record_container(prepare_dir, match.group(1))

        build = [str(ctx.src / ".build/docker/build-jars.sh"), ctx.jdk]
        exit_code = self.run_logged(build, prepare_dir, {}, on_line, cwd=ctx.src)
        if exit_code != 0:
            raise PluginError(f"build-jars.sh failed with exit code {exit_code}; see {prepare_dir / 'console.log'}")

    def setup_slot(self, ctx: RunContext, slot_dir: Path) -> None:
        trash = ctx.trash_dir / slot_dir.name
        sync_tree(ctx.src, slot_dir / "src", trash / "src", SLOT_EXCLUDES)
        m2 = Path.home() / ".m2" / "repository"
        m2.mkdir(parents=True, exist_ok=True)
        (slot_dir / "m2").mkdir(exist_ok=True)
        subprocess.run(["rsync", "-a", f"{m2}/", f"{slot_dir / 'm2'}/"], check=True)
        dtest = ctx.src.parent / "cassandra-dtest"
        if dtest.is_dir():
            sync_tree(dtest, slot_dir / "cassandra-dtest", trash / "cassandra-dtest")
        limits.patch(slot_dir / "src/.build/docker/run-tests.sh", ctx.memory)

    def jobs(self, ctx: RunContext) -> list[Job]:
        jenkins = ctx.src / ".jenkins/Jenkinsfile"
        if not jenkins.is_file():
            raise PluginError(f"{jenkins} doesn't exist")
        return jenkinsfile.select_jobs(
            jenkins.read_text(errors="replace"),
            ctx.profile,
            ctx.jdk,
            self.default_jdk(ctx.src),
            self.options(ctx.plugin_args).with_upgrade,
        )

    def first_job(self, jobs: list[Job]) -> Job | None:
        """Run stress-test first and alone, so the test image is built once."""
        return next((job for job in jobs if job.group == "stress-test"), None)

    def class_files(self, src: Path, group: str) -> list[str]:
        """Return the test class files of a group relative to its test directory, sorted, as run-tests.sh lists them."""
        if group not in self.class_cache:
            directory = "unit" if group in UNIT_GROUPS else CLASS_DIRS.get(group)
            found = []
            if directory:
                root = src / "test" / directory
                found = sorted(str(path.relative_to(root)) for path in root.rglob("*Test.java"))
                if group == "jvm-dtest":
                    found = [path for path in found if "upgrade" not in path]
            self.class_cache[group] = found
        return self.class_cache[group]

    def test_classes(self, ctx: RunContext, job: Job) -> list[str]:
        """Return the classes of split K/N, every Nth class from the Kth like split -n r/K/N in run-tests.sh."""
        target, split = job.args
        match = re.fullmatch(r"(\d+)/(\d+)", split)
        if not match:
            return []
        index, count = map(int, match.groups())
        return self.class_files(ctx.src, target)[index - 1 :: count]

    def size(self, src: Path, group: str) -> tuple[int, str]:
        """Return the CPUs and memory of the Jenkins agent size of a test step."""
        if not self.size_cache:
            self.size_cache = jenkinsfile.read_sizes((src / ".jenkins/Jenkinsfile").read_text(errors="replace"))
        return SIZES.get(self.size_cache.get(group, "medium"), SIZES["medium"])

    def job_memory(self, ctx: RunContext, job: Job) -> int:
        """Return -m if given, else the memory run-tests.sh gives the job's target."""
        return parse_size(ctx.memory or self.size(ctx.src, job.group)[1])

    def run_job(self, ctx: RunContext, slot_dir: Path, job: Job, job_dir: Path, classes: list[str] | None = None) -> int:
        target, split = job.args
        env = {
            "m2_dir": str(slot_dir / "m2"),
            "cassandra_dtest_dir": str(slot_dir / "cassandra-dtest"),
            "docker_timeout_hours": str(DOCKER_TIMEOUT_HOURS),
            "docker_cpus": str(self.size(ctx.src, job.group)[0]),
            "SKIP_DOCKER_DEBUG_ON_FAIL": "1",
            "python_version": "3.8",
        }

        def on_line(line: str) -> None:
            match = CONTAINER_LINE.match(line)
            if match:
                self.record_container(job_dir, match.group(1))

        script = slot_dir / "src/.build/docker/run-tests.sh"
        selection = ["-t", ",".join(classes)] if classes else ["-c", split]
        cmd = [str(script), "-a", target, *selection, "-j", ctx.jdk]
        return self.run_logged(cmd, job_dir, env, on_line, cwd=slot_dir / "src")

    def collect_results(self, ctx: RunContext, slot_dir: Path, job: Job, job_dir: Path) -> None:
        build = slot_dir / "src/build"
        started = job_start(job_dir)
        since = started["start"] if started else 0.0
        results = job_dir / "results"
        _copy_newer(build / "test/output", results / "output", since)
        _copy_newer(build / "test/logs", results / "logs", since)
        test_list = build / "test_list.txt"
        if test_list.is_file() and test_list.stat().st_mtime >= since - 1:
            results.mkdir(parents=True, exist_ok=True)
            shutil.copy2(test_list, results / "test_list.txt")
            self.record_test_total(ctx, job.group, test_list)

    def record_test_total(self, ctx: RunContext, group: str, test_list: Path) -> None:
        """Store the group's test total, once per group: the tests its splits in this run take from the dtest list.

        test_list.txt lists the whole suite, which run-python-dtests.sh deals out with split -n r/K/N.
        """
        files = RunFiles(ctx.run_dir)
        with self.totals_lock:
            totals = read_json(files.totals, {})
            tests = totals.setdefault("tests_per_group", {})
            if group not in tests:
                listed = sum(1 for line in test_list.read_text().splitlines() if line.strip())
                splits = [job["args"][-1] for job in read_json(files.jobs, []) if job["group"] == group]
                tests[group] = sum(split_size(listed, split) for split in splits)
                write_json(files.totals, totals)

    def in_progress_results(self, slot_dir: Path, since: float) -> list[Path]:
        found = (slot_dir / "src/build/test/output").rglob("TEST-*.xml")
        return sorted(path for path in found if path.stat().st_mtime >= since)

    def activity(self, lines: list[str]) -> list[str]:
        """Return the test class ant runs now and the latest line that isn't a JVM warning."""
        suite = next((match.group(1) for line in reversed(lines) if (match := TESTSUITE_LINE.search(line))), None)
        latest = next((line for line in reversed(lines) if not JVM_WARNING.search(line)), None)
        shown = [f"class {suite}"] if suite else []
        if latest and not TESTSUITE_LINE.search(latest):
            shown.append(latest)
        return shown

    def crash_cause(self, lines: list[str]) -> str:
        """Return the first error after the last Cassandra log line before ant's BUILD FAILED, as later output can
        be a follow-on error; without one, the last line that isn't a shell trace."""
        end = max((index for index, line in enumerate(lines) if line.startswith("BUILD FAILED")), default=len(lines))
        texts = [ANT_TASK.sub("", line).strip() for line in lines[:end]]
        start = max((index + 1 for index, text in enumerate(texts) if LOG_LINE.match(text)), default=0)
        error = next((text for text in texts[start:] if ERROR_LINE.search(text)), None)
        return error or next((text for text in reversed(texts) if text and not text.startswith("+ ")), "")

    def totals(self, ctx: RunContext, jobs: list[Job]) -> Totals | None:
        classes = Counter()
        for job in jobs:
            classes[job.group] += len(self.test_classes(ctx, job))
        return Totals(classes_per_group={group: count for group, count in classes.items() if count})

    def report(self, ctx: RunContext, crashed: list[dict]) -> None:
        """Build ci_summary.html and results_details.tar.xz like generateTestReports() in the Jenkinsfile, and add
        the crashed jobs to ci_summary.html."""
        build = ctx.run_dir / "report-build"
        output = build / "test/output"
        for job_dir in sorted((ctx.run_dir / "jobs").iterdir()):
            record = job_record(job_dir)
            if not record:
                continue
            write_cell_time(build / "test/cell-times", job_dir, record, ctx.jdk)
            source = job_dir / "results/output"
            if not source.is_dir():
                continue
            group = record["group"]
            for path in source.rglob("*.xml"):
                relative = path.relative_to(source)
                if relative.parts[0] == group:
                    relative = Path(*relative.parts[1:])
                if path.name.startswith("nosetests"):
                    relative = relative.with_name(f"nosetests_{job_dir.name}.xml")
                destination = output / group / relative
                destination.parent.mkdir(parents=True, exist_ok=True)
                shutil.copy2(path, destination)
        if not output.is_dir():
            return

        docker_run = str(ctx.src / ".build/docker/_docker_run.sh")
        for target in sorted(path.name for path in output.iterdir() if path.is_dir()):
            ant_options = (
                f"-Xmx2g -Dbuild.test.output.dir=/dist/test/output/{target}"
                f" -Dbuild.test.report.dir=/dist/test/reports/{target}"
            )
            self.run_report(ctx, build, [docker_run, "debian-build.docker", "ci/generate-test-report.sh"], ant_options)
        self.run_report(ctx, build, [docker_run, "debian-build.docker", "ci/generate-ci-summary.sh"])

        report = ctx.run_dir / "report"
        report.mkdir(exist_ok=True)
        if (build / "test/reports").is_dir():
            subprocess.run(["tar", "-cJf", str(report / "results_details.tar.xz"), "-C", str(build / "test"), "reports"])
        if (build / "ci_summary.html").is_file():
            if crashed:
                write_into_body(build / "ci_summary.html", crashed_html(crashed, ctx.run_dir))
            shutil.copy2(build / "ci_summary.html", report / "ci_summary.html")

    @staticmethod
    def run_report(ctx: RunContext, build: Path, cmd: list[str], ant_options: str = "") -> None:
        env = {**os.environ, "build_dir": str(build), "CASSANDRA_DOCKER_ANT_OPTS": ant_options}
        result = subprocess.run(cmd, cwd=ctx.src, env=env)
        if result.returncode != 0:
            print(f"{' '.join(cmd[1:])} failed with exit code {result.returncode}", flush=True)


PLUGIN = CassandraPlugin
