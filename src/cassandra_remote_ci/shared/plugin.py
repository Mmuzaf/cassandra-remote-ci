# SPDX-License-Identifier: Apache-2.0
"""Plugin base class, job and run descriptions, and the plugin loader."""

from __future__ import annotations

import importlib
import os
import pkgutil
import re
import subprocess
from abc import ABC, abstractmethod
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Callable

from cassandra_remote_ci import plugins

SIZE_UNITS = {"": 1, "b": 1, "k": 2**10, "m": 2**20, "g": 2**30, "t": 2**40}


class PluginError(Exception):
    """A plugin can't do what was asked, with a message for the user."""


def parse_size(text: str) -> int:
    """Return the bytes in a docker memory size such as 16g or 512m."""
    match = re.fullmatch(r"(\d+)([bkmgt]?)", text.lower())
    if not match:
        raise PluginError(f"can't read size {text!r}; use a number with k, m or g, e.g. 16g")
    return int(match.group(1)) * SIZE_UNITS[match.group(2)]


@dataclass(frozen=True)
class Job:
    id: str
    group: str
    args: list[str]

    def line(self) -> str:
        """Return the text that --only matches against."""
        return " ".join([self.id, *self.args])

    def to_dict(self) -> dict:
        return asdict(self)

    @staticmethod
    def from_dict(data: dict) -> Job:
        return Job(data["id"], data["group"], list(data["args"]))


def filter_jobs(jobs: list[Job], only: str | None) -> list[Job]:
    if not only:
        return jobs
    pattern = re.compile(only)
    return [job for job in jobs if pattern.search(job.line())]


@dataclass(frozen=True)
class RunContext:
    run_id: str
    run_dir: Path
    src: Path
    jdk: str
    memory: str
    parallel: int
    profile: str
    plugin_args: list[str]
    trash_dir: Path = Path("/tmp/cassandra-remote-ci-trash")


@dataclass
class Totals:
    jobs_per_group: dict[str, int] = field(default_factory=dict)
    classes_per_group: dict[str, int] = field(default_factory=dict)
    tests_per_group: dict[str, int] = field(default_factory=dict)


def sync_tree(src: Path, dst: Path, trash: Path, excludes: tuple[str, ...] = ()) -> None:
    """Mirror src into dst with rsync; files rsync would remove or overwrite move into trash."""
    dst.mkdir(parents=True, exist_ok=True)
    cmd = ["rsync", "-a", "--delete", "--backup", f"--backup-dir={trash}"]
    cmd += [f"--exclude={pattern}" for pattern in excludes]
    subprocess.run([*cmd, f"{src}/", f"{dst}/"], check=True)


class Plugin(ABC):
    name: str = ""
    default_parallel = 1
    default_memory = "16g"
    default_profile = ""
    sync_excludes: list[str] = []
    remote_commands: list[str] = []

    @abstractmethod
    def detect(self, src: Path) -> bool:
        """Return True if src is a checkout this plugin can test."""

    @abstractmethod
    def default_jdk(self, src: Path) -> str:
        """Return the JDK to use when -j isn't given."""

    def extra_sources(self, src: Path, plugin_args: list[str]) -> dict[str, Path]:
        """Return local directories to sync next to the source tree, keyed by remote directory name."""
        return {}

    def prepare(self, ctx: RunContext) -> None:
        """Build whatever the jobs share, once per run, in ctx.src."""

    def setup_slot(self, ctx: RunContext, slot_dir: Path) -> None:
        """Give a slot its own working copy."""
        sync_tree(ctx.src, slot_dir / "src", ctx.trash_dir / slot_dir.name / "src")

    @abstractmethod
    def jobs(self, ctx: RunContext) -> list[Job]:
        """Return every job of the run."""

    def first_job(self, jobs: list[Job]) -> Job | None:
        """Return a job to run alone before the others, for example to build a shared image."""
        return None

    def test_classes(self, ctx: RunContext, job: Job) -> list[str]:
        """Return the test classes of a job that can run in parts on several slots; empty runs the job whole."""
        return []

    def job_memory(self, ctx: RunContext, job: Job) -> int:
        """Return the bytes of memory a job's container may use, which the run reserves from its budget meanwhile."""
        return parse_size(ctx.memory or self.default_memory)

    @abstractmethod
    def run_job(self, ctx: RunContext, slot_dir: Path, job: Job, job_dir: Path, classes: list[str] | None = None) -> int:
        """Run a job, or only the given classes of it, in slot_dir; stream output to job_dir/console.log; return the exit code."""

    @abstractmethod
    def collect_results(self, ctx: RunContext, slot_dir: Path, job: Job, job_dir: Path) -> None:
        """Copy JUnit-format XML and logs from slot_dir into job_dir/results."""

    def activity(self, lines: list[str]) -> list[str]:
        """Return what a running job is doing, from the last non-blank lines of its console log."""
        return lines[-1:]

    def crash_cause(self, lines: list[str]) -> str:
        """Return why a job exited non-zero without a failing test, from the lines of its console log."""
        return next((line.strip() for line in reversed(lines) if line.strip()), "")

    def in_progress_results(self, slot_dir: Path, since: float) -> list[Path]:
        """Return result files the job running in slot_dir wrote since it started: JUnit XML, or events files of
        status, class, method lines."""
        return []

    def totals(self, ctx: RunContext, jobs: list[Job]) -> Totals | None:
        """Return the expected number of classes and tests per group, where known."""
        return None

    def report(self, ctx: RunContext, crashed: list[dict]) -> None:
        """Write project-specific reports into ctx.run_dir/report; crashed are the crashed jobs of collect_failures."""

    def record_container(self, job_dir: Path, container_id: str) -> None:
        """Note a container started for the job, so abort can stop it."""
        with open(job_dir / "containers", "a") as out:
            out.write(container_id + "\n")

    def run_logged(
        self,
        cmd: list[str],
        job_dir: Path,
        env: dict[str, str],
        on_line: Callable[[str], None] | None = None,
        cwd: Path | None = None,
    ) -> int:
        """Run cmd, append its output to job_dir/console.log, and call on_line for each line."""
        job_dir.mkdir(parents=True, exist_ok=True)
        with open(job_dir / "console.log", "a") as log:
            log.write("$ " + " ".join(cmd) + "\n")
            log.flush()
            with subprocess.Popen(
                cmd,
                cwd=cwd,
                env={**os.environ, **env},
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                stdin=subprocess.DEVNULL,
                text=True,
                errors="replace",
                bufsize=1,
            ) as process:
                for line in process.stdout:
                    log.write(line)
                    log.flush()
                    if on_line:
                        on_line(line.rstrip("\n"))
                return process.wait()


def available_plugins() -> dict[str, type[Plugin]]:
    """Import every module under cassandra_remote_ci.plugins and return its PLUGIN by name."""
    found = {}
    for module in pkgutil.iter_modules(plugins.__path__):
        plugin_class = importlib.import_module(f"{plugins.__name__}.{module.name}").PLUGIN
        found[plugin_class.name] = plugin_class
    return found


def load_plugin(name: str) -> Plugin:
    known = available_plugins()
    if name not in known:
        raise PluginError(f"unknown plugin {name!r}; known plugins are {', '.join(sorted(known))}")
    return known[name]()


def detect_plugin(src: Path) -> Plugin:
    """Return the one plugin whose detect() accepts src."""
    matches = [plugin for plugin in (cls() for cls in available_plugins().values()) if plugin.detect(src)]
    if not matches:
        raise PluginError(f"no plugin recognises {src}; pass -P <plugin>")
    if len(matches) > 1:
        names = ", ".join(plugin.name for plugin in matches)
        raise PluginError(f"several plugins recognise {src} ({names}); pass -P <plugin>")
    return matches[0]
