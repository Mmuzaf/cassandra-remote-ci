# SPDX-License-Identifier: Apache-2.0
"""Durations and test counts of finished jobs and prepare phases, and the ETA computed from them.

They live in stats/jobs.jsonl, outside runs/, so cleanup keeps them.
"""

from __future__ import annotations

import json
import math
import os
from pathlib import Path
from statistics import median
from typing import Iterator

from cassandra_remote_ci.shared.state import job_record, list_runs, read_json, run_files

RECENT = 5
MATCHED = ("plugin", "profile", "jdk")
KILLED_EXIT_CODES = (-15, -9, 137, 143)


def stats_file(base_dir: Path) -> Path:
    return base_dir / "stats" / "jobs.jsonl"


def _run_fields(info: dict) -> dict:
    return {"run_id": info.get("run_id"), "parallel": info.get("parallel"), **{key: info.get(key) for key in MATCHED}}


def job_stats(info: dict, record: dict, tests: int, failures: int, work: float) -> dict:
    """Return the record of a finished job; work is the slot time it took, the sum over its parts."""
    return {
        **_run_fields(info),
        "type": "job",
        "job": record["id"],
        "group": record["group"],
        "start": record["start"],
        "end": record["end"],
        "work": work,
        "divisible": bool(record.get("parts")),
        "exit_code": record["exit_code"],
        "tests": tests,
        "failures": failures,
        "crashed": record["exit_code"] != 0 and not failures,
        "peak_memory": record.get("peak_memory"),
    }


def interrupted_stats(info: dict, job_id: str, group: str, start: float, end: float, work: float) -> dict:
    """Return the record of a job cut short by abort; its work is a lower bound."""
    return {**_run_fields(info), "type": "job", "job": job_id, "group": group, "start": start, "end": end,
            "work": work, "interrupted": True}


def work_of(record: dict) -> float:
    return record.get("work", record["end"] - record["start"])


def prepare_stats(info: dict, start: float, end: float) -> dict:
    return {**_run_fields(info), "type": "prepare", "start": start, "end": end}


def is_interrupted(record: dict) -> bool:
    """Return whether a job was cut short, by abort or a kill, even with failures already reported."""
    return bool(record.get("interrupted") or record.get("exit_code") in KILLED_EXIT_CODES)


def from_runs(base_dir: Path, skip_run: str | None = None) -> Iterator[dict]:
    """Yield the records of the runs under runs/, for a stats file that doesn't exist yet."""
    for run_id in list_runs(base_dir):
        if run_id == skip_run:
            continue
        files = run_files(base_dir, run_id)
        info = read_json(files.info, {})
        records = []
        for path in sorted((files.run_dir / "jobs").glob("*/job.json")):
            record = job_record(path.parent)
            if record:
                tests = read_json(path.parent / "counts.json", {}).get("tests", 0)
                failures = len(read_json(path.parent / "failures.json", []))
                records.append(job_stats(info, record, tests, failures, record["end"] - record["start"]))
        preparing = read_json(files.state, {}).get("since", {}).get("preparing")
        if preparing and records:
            yield prepare_stats(info, preparing, min(record["start"] for record in records))
        yield from sorted(records, key=lambda record: record["end"])


def create(base_dir: Path, run_id: str) -> None:
    """Create a missing stats file from the runs under runs/ other than run_id."""
    path = stats_file(base_dir)
    if path.exists():
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f"{path.name}.{os.getpid()}.tmp")
    temporary.write_text("".join(json.dumps(record, sort_keys=True) + "\n" for record in from_runs(base_dir, run_id)))
    os.replace(temporary, path)


def append(base_dir: Path, record: dict) -> None:
    """Append a record with one write and no lock, so a signal handler can call it at any point."""
    path = stats_file(base_dir)
    path.parent.mkdir(parents=True, exist_ok=True)
    out = os.open(path, os.O_WRONLY | os.O_APPEND | os.O_CREAT, 0o644)
    try:
        os.write(out, (json.dumps(record, sort_keys=True) + "\n").encode())
    finally:
        os.close(out)


def read_stats(base_dir: Path) -> list[dict]:
    """Return the records in the order they were written, or those of runs/ if there is no stats file yet."""
    try:
        lines = stats_file(base_dir).read_text().splitlines()
    except FileNotFoundError:
        return list(from_runs(base_dir))
    records = []
    for line in lines:
        try:
            records.append(json.loads(line))
        except json.JSONDecodeError:
            continue
    return records


class Estimates:
    """Expected durations and test counts, from the records of runs with the same plugin, profile and JDK."""

    def __init__(self, records: list[dict], info: dict):
        matching = [record for record in records if all(record.get(key) == info.get(key) for key in MATCHED)]
        self.prepare = [record["end"] - record["start"] for record in matching if record["type"] == "prepare"]
        all_jobs = [record for record in matching if record["type"] == "job"]
        jobs = [record for record in all_jobs if not record.get("crashed") and not is_interrupted(record)]
        self.by_job: dict[str, list[dict]] = {}
        self.by_group: dict[str, list[float]] = {}
        self.at_least: dict[str, float] = {}
        for record in jobs:
            self.by_job.setdefault(record["job"], []).append(record)
            self.by_group.setdefault(record["group"], []).append(work_of(record))
        for record in filter(is_interrupted, all_jobs):
            self.at_least[record["job"]] = max(self.at_least.get(record["job"], 0.0), work_of(record))
        last_run = next((record["run_id"] for record in reversed(all_jobs) if record["run_id"] != info.get("run_id")), None)
        last = {record["job"]: record for record in all_jobs if record["run_id"] == last_run}
        self.last_run_jobs = [(job, record["group"], 1.0, bool(record.get("divisible"))) for job, record in last.items()]

    def duration(self, job_id: str, group: str) -> float | None:
        """Return the median of the recent work of the job, else of its group, in slot seconds, or None.

        Jobs of other groups take too different times to stand in. Without a finished record of its own, a job
        takes at least its longest interrupted run.
        """
        own = [work_of(record) for record in self.by_job.get(job_id, [])]
        durations = own or self.by_group.get(group)
        estimate = median(durations[-RECENT:]) if durations else None
        if not own and job_id in self.at_least:
            return max(estimate or 0.0, self.at_least[job_id])
        return estimate

    def prepare_duration(self) -> float | None:
        return median(self.prepare[-RECENT:]) if self.prepare else None

    def tests(self, job_id: str) -> int | None:
        records = self.by_job.get(job_id)
        return records[-1]["tests"] if records else None


def _fill(slots: list[float], work: float) -> float:
    """Return when slots busy until the given times finish, with work spread over them to end as early as possible."""
    ordered = sorted(slots)
    for count in range(1, len(ordered) + 1):
        level = (sum(ordered[:count]) + work) / count
        if count == len(ordered) or level <= ordered[count]:
            return max(level, ordered[-1])
    return 0.0


def _start(busy: list[tuple[float, int]], after: float, need: int, parallel: int, budget: float) -> float:
    """Return the first time from after when busy, the (end, memory) of the jobs started by then, leaves a slot and
    need of the budget free, or nothing runs."""
    for moment in sorted({after, *(end for end, _ in busy if end > after)}):
        active = [memory for end, memory in busy if end > moment]
        if not active or (len(active) < parallel and sum(active) + need <= budget):
            return moment
    return after


def eta(
    estimates: Estimates,
    waiting: list[tuple[str, str, float, bool]],
    running: list[tuple[str, str, float, float]],
    parallel: int,
    before: float = 0.0,
    memory: dict[str, int] | None = None,
    budget: float = math.inf,
) -> float | None:
    """Return the seconds until the last job ends, or None without any earlier record.

    waiting holds (job, group, share of the job left to start, divisible); running holds (job, group, share of the
    job, elapsed seconds) for each job or part. A whole waiting job starts, in queue order, once a slot is free and
    its memory fits the budget; divisible work fills all slots evenly, as parts do. before is the time until jobs
    can start, e.g. rest of prepare.
    """
    expected = {job: estimates.duration(job, group) for job, group, *_ in [*waiting, *running]}
    if None in expected.values():
        return None
    memory = memory or {}
    busy = [(max(expected[job] * share - elapsed, 0.0), memory.get(job, 0)) for job, _, share, elapsed in running]
    start, divisible_work = 0.0, 0.0
    for job, _, share, divisible in waiting:
        if divisible:
            divisible_work += expected[job] * share
            continue
        start = _start(busy, start, memory.get(job, 0), parallel, budget)
        busy = [item for item in busy if item[0] > start] + [(start + expected[job] * share, memory.get(job, 0))]
    slots = sorted(end for end, _ in busy)[-parallel:]
    return before + _fill(slots + [start] * (parallel - len(slots)), divisible_work)
