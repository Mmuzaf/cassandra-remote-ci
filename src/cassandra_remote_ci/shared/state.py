# SPDX-License-Identifier: Apache-2.0
"""Run state on the box: run.json, state.json, the lock, and the files of each job."""

from __future__ import annotations

import json
import os
import subprocess
import threading
import time
from dataclasses import dataclass
from pathlib import Path

from cassandra_remote_ci.shared.plugin import RunContext

UNIT_PREFIX = "cassandra-remote-ci-"
FINISHED_PHASES = ("done", "failed", "aborted")
# Exit code of the box's status command once the run has stopped.
FINISHED_EXIT = 3
PART_PREFIX = "part-"
DEFAULT_MIN_CLASSES_PER_PART = 10


def unit_name(run_id: str) -> str:
    return UNIT_PREFIX + run_id


def write_json(path: Path, data) -> None:
    """Write JSON through a temporary file, so readers never see half a file."""
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f"{path.name}.{os.getpid()}.{threading.get_ident()}.tmp")
    temporary.write_text(json.dumps(data, indent=2, sort_keys=True) + "\n")
    os.replace(temporary, path)


def read_json(path: Path, default=None):
    try:
        return json.loads(path.read_text())
    except (FileNotFoundError, json.JSONDecodeError):
        return default


@dataclass(frozen=True)
class RunFiles:
    run_dir: Path

    @property
    def base_dir(self) -> Path:
        return self.run_dir.parent.parent

    @property
    def info(self) -> Path:
        return self.run_dir / "run.json"

    @property
    def state(self) -> Path:
        return self.run_dir / "state.json"

    @property
    def jobs(self) -> Path:
        return self.run_dir / "jobs.json"

    @property
    def totals(self) -> Path:
        return self.run_dir / "totals.json"

    @property
    def progress(self) -> Path:
        return self.run_dir / "progress.json"

    @property
    def failures_txt(self) -> Path:
        return self.run_dir / "failures.txt"

    @property
    def failures_json(self) -> Path:
        return self.run_dir / "failures.json"

    @property
    def crashed(self) -> Path:
        return self.run_dir / "crashed-jobs.txt"

    @property
    def summary_md(self) -> Path:
        return self.run_dir / "summary.md"

    @property
    def summary_json(self) -> Path:
        return self.run_dir / "summary.json"

    @property
    def report(self) -> Path:
        return self.run_dir / "report"

    @property
    def usage(self) -> Path:
        return self.run_dir / "usage.json"

    def job_dir(self, job_id: str) -> Path:
        return self.run_dir / "jobs" / job_id


def run_files(base_dir: Path, run_id: str) -> RunFiles:
    return RunFiles(base_dir / "runs" / run_id)


def context_for(base_dir: Path, info: dict) -> RunContext:
    """Build the RunContext of a run on the box from its run.json."""
    return RunContext(
        run_id=info["run_id"],
        run_dir=base_dir / "runs" / info["run_id"],
        src=base_dir / "src",
        jdk=info["jdk"],
        memory=info["memory"],
        parallel=int(info["parallel"]),
        profile=info["profile"],
        plugin_args=list(info.get("plugin_args", [])),
        trash_dir=Path(info["trash_dir"]),
    )


def slot_path(ctx: RunContext, slot: int) -> Path:
    return ctx.src.parent / f"slot{slot}"


def set_phase(files: RunFiles, phase: str, error: str | None = None) -> None:
    state = read_json(files.state, {})
    state["phase"] = phase
    state.setdefault("since", {})[phase] = time.time()
    if error:
        state["error"] = error
    write_json(files.state, state)


def phase_of(files: RunFiles) -> str:
    return read_json(files.state, {}).get("phase", "unknown")


def read_lock(base_dir: Path) -> str:
    try:
        return (base_dir / "lock").read_text().strip()
    except FileNotFoundError:
        return ""


def write_lock(base_dir: Path, run_id: str) -> None:
    (base_dir / "lock").write_text(run_id + "\n" if run_id else "")


def release_lock(base_dir: Path, run_id: str) -> None:
    """Empty the lock file if it names run_id."""
    if read_lock(base_dir) == run_id:
        write_lock(base_dir, "")


def unit_state(run_id: str) -> str:
    try:
        result = subprocess.run(
            ["systemctl", "--user", "show", "-p", "ActiveState", "--value", unit_name(run_id)],
            capture_output=True,
            text=True,
        )
    except FileNotFoundError:
        return "unknown"
    return result.stdout.strip() or "unknown"


def active_run(base_dir: Path) -> str:
    """Return the run named in the lock if its unit is still running, else an empty string."""
    run_id = read_lock(base_dir)
    if run_id and unit_state(run_id) in ("active", "activating", "deactivating"):
        return run_id
    return ""


def list_runs(base_dir: Path) -> list[str]:
    runs = base_dir / "runs"
    if not runs.is_dir():
        return []
    return sorted(path.name for path in runs.iterdir() if (path / "run.json").exists())


def resolve_run(base_dir: Path, run_id: str | None) -> str:
    """Return run_id, else the run named in the lock, else the newest run."""
    run_id = run_id or read_lock(base_dir)
    if not run_id:
        runs = list_runs(base_dir)
        if not runs:
            raise LookupError(f"no runs in {base_dir / 'runs'}")
        run_id = runs[-1]
    if not run_files(base_dir, run_id).info.exists():
        raise LookupError(f"{base_dir / 'runs'} has no run {run_id}")
    return run_id


def job_record(job_dir: Path) -> dict | None:
    return read_json(job_dir / "job.json")


def job_start(job_dir: Path) -> dict | None:
    return read_json(job_dir / "started.json")


def part_dirs(job_dir: Path) -> list[Path]:
    """Return the part-<n> directories of a job's latest attempt, in order."""
    if not job_dir.is_dir():
        return []
    found = [path for path in job_dir.iterdir() if path.name.startswith(PART_PREFIX) and path.name[len(PART_PREFIX) :].isdigit()]
    return sorted(found, key=lambda path: int(path.name[len(PART_PREFIX) :]))


def attempt_number(job_dir: Path) -> int:
    """Return the number of the next attempt of a job, one more than its attempt-<k> directories."""
    if not job_dir.is_dir():
        return 1
    return 1 + sum(1 for path in job_dir.iterdir() if path.name.startswith("attempt-"))


def move_to_attempt(job_dir: Path) -> None:
    """Move the files of the latest attempt into job_dir/attempt-<k>."""
    current = [path for path in job_dir.iterdir() if not path.name.startswith("attempt-")]
    if not current:
        return
    target = job_dir / f"attempt-{attempt_number(job_dir)}"
    target.mkdir()
    for path in current:
        path.rename(target / path.name)
