# SPDX-License-Identifier: Apache-2.0
"""The status snapshot of a run: jobs and tests done against the totals, the ETA, and recent failures."""

from __future__ import annotations

import math
import os
import re
import statistics
import time
from pathlib import Path

from cassandra_remote_ci.box import stats
from cassandra_remote_ci.shared import junit
from cassandra_remote_ci.shared.junit import Counts
from cassandra_remote_ci.shared.plugin import Job, Plugin, PluginError, RunContext, filter_jobs
from cassandra_remote_ci.shared.state import (
    FINISHED_PHASES,
    PART_PREFIX,
    RunFiles,
    job_record,
    job_start,
    part_dirs,
    read_json,
    slot_path,
    write_json,
)

RECENT_FAILURES = 5
LINE_WIDTH = 160
TAIL_BYTES = 65536
ANSI = re.compile(r"\x1b\[[0-9;?]*[A-Za-z]")


def clean_lines(text: str) -> list[str]:
    """Return the non-blank lines of text without ANSI codes."""
    return [stripped for line in text.splitlines() if (stripped := ANSI.sub("", line).strip())]


def last_line(text: str) -> str | None:
    """Return the last non-blank line of text without ANSI codes, cut to LINE_WIDTH."""
    lines = clean_lines(text)
    return lines[-1][:LINE_WIDTH] if lines else None


def log_tail(path: Path) -> list[str]:
    """Return the non-blank lines of the last TAIL_BYTES of a log file, none if it can't be read."""
    try:
        with open(path, "rb") as log:
            log.seek(max(log.seek(0, os.SEEK_END) - TAIL_BYTES, 0))
            return clean_lines(log.read().decode(errors="replace"))
    except OSError:
        return []


def _elapsed(state: dict, now: float) -> float:
    since = state.get("since", {})
    if not since:
        return 0.0
    start = min(since.values())
    end = since.get(state.get("phase"), now) if state.get("phase") in FINISHED_PHASES else now
    return max(end - start, 0.0)


def snapshot(plugin: Plugin, ctx: RunContext, files: RunFiles) -> dict:
    """Read the files of a run and return its progress as a JSON-ready dict."""
    now = time.time()
    info = read_json(files.info, {})
    state = read_json(files.state, {})
    jobs = [Job.from_dict(data) for data in read_json(files.jobs, [])]
    totals = read_json(files.totals, {})
    estimates = stats.Estimates(stats.read_stats(files.base_dir), info)

    groups: dict[str, dict] = {}
    for job in jobs:
        groups.setdefault(job.group, {"jobs": 0, "jobs_total": 0, "counts": Counts(), "expected_tests": []})
        groups[job.group]["jobs_total"] += 1
        groups[job.group]["expected_tests"].append(estimates.tests(job.id))

    running, waiting, recent, crashed = [], [], [], 0
    for job in jobs:
        job_dir = files.job_dir(job.id)
        group = groups[job.group]
        record = job_record(job_dir)
        if record:
            group["jobs"] += 1
            group["peak_memory"] = max(group.get("peak_memory") or 0, record.get("peak_memory") or 0) or None
            group["counts"].add(Counts.from_dict(read_json(job_dir / "counts.json")))
            failures = read_json(job_dir / "failures.json", [])
            crashed += record["exit_code"] != 0 and not failures
            recent += [(record["end"], job, junit.TestResult(**failure)) for failure in failures]
            continue
        expected = estimates.duration(job.id, job.group)
        classes = plugin.test_classes(ctx, job)

        def add_running(started: dict, share: float, **part) -> None:
            elapsed = now - started["start"]
            running.append({"job": job.id, "group": job.group, "slot": started["slot"], "elapsed": elapsed, "share": share,
                            "expected": None if expected is None else expected * share, **part})
            results = junit.parse_results(plugin.in_progress_results(slot_path(ctx, started["slot"]), started["start"]))
            group["counts"].add(junit.count(results))
            recent.extend((now, job, result) for result in junit.failing(results))

        parts = part_dirs(job_dir)
        if parts:
            started_classes = 0
            for part_dir in parts:
                started = job_start(part_dir) or {}
                part_classes = len(started.get("classes", []))
                started_classes += part_classes
                ended = read_json(part_dir / "part.json")
                if ended:
                    group["counts"].add(Counts.from_dict(read_json(part_dir / "counts.json")))
                    failures = read_json(part_dir / "failures.json", [])
                    recent += [(ended["end"], job, junit.TestResult(**failure)) for failure in failures]
                elif started:
                    share = part_classes / max(len(classes), 1)
                    add_running(started, share, part=int(part_dir.name[len(PART_PREFIX) :]), classes=part_classes)
            if started_classes < len(classes):
                waiting.append((job.id, job.group, 1 - started_classes / len(classes), True))
            continue
        started = job_start(job_dir)
        if started:
            add_running(started, 1.0)
        else:
            waiting.append((job.id, job.group, 1.0, bool(classes)))

    group_rows = []
    for name, group in groups.items():
        counts = group["counts"]
        tests_total = totals.get("tests_per_group", {}).get(name)
        expected_tests = [tests for tests in group["expected_tests"] if tests is not None]
        unknown = len(group["expected_tests"]) - len(expected_tests)
        if expected_tests and unknown:
            expected_tests.append(round(statistics.median(expected_tests) * unknown))
        group_rows.append(
            {
                "group": name,
                "jobs": group["jobs"],
                "jobs_total": group["jobs_total"],
                "classes": counts.classes,
                "classes_total": totals.get("classes_per_group", {}).get(name),
                "tests": counts.tests,
                "tests_total": tests_total,
                "tests_estimate": None if tests_total else sum(expected_tests) or None,
                "failures": counts.failures,
                "errors": counts.errors,
                "skipped": counts.skipped,
                "peak_memory": group.get("peak_memory"),
            }
        )

    recent.sort(key=lambda item: item[0])
    done = sum(row["jobs"] for row in group_rows)
    phase = state.get("phase", "unknown")
    planned = planned_jobs(plugin, ctx, info.get("only")) if not jobs and phase == "preparing" else jobs
    if planned is not jobs:
        waiting = [(job.id, job.group, 1.0, bool(plugin.test_classes(ctx, job))) for job in planned]
    try:
        memory = {job.id: plugin.job_memory(ctx, job) for job in planned}
    except (PluginError, OSError):
        memory = {}
    budget = info.get("memory_budget") or math.inf
    for row in group_rows:
        row["memory"] = max((memory.get(job.id, 0) for job in jobs if job.group == row["group"]), default=0) or None
    return {
        "run_id": ctx.run_id,
        "plugin": info.get("plugin"),
        "phase": phase,
        "error": state.get("error"),
        "elapsed": _elapsed(state, now),
        "eta": _eta(estimates, phase, state, waiting, running, ctx.parallel, now, memory, budget),
        "parallel": ctx.parallel,
        "memory_budget": info.get("memory_budget"),
        "memory_reserved": sum(memory.get(item["job"], 0) for item in running),
        "usage": read_json(files.usage) if phase == "running" else None,
        "jobs": {"done": done, "total": len(jobs), "crashed": crashed, "running": running},
        "groups": group_rows,
        "recent": [
            {"group": job.group, "test": result.test, "status": result.status, "job": job.id}
            for _, job, result in recent[-RECENT_FAILURES:]
        ],
        "updated": now,
    }


def planned_jobs(plugin: Plugin, ctx: RunContext, only: str | None) -> list[Job]:
    """Return the jobs prepare will queue, or none if the plugin can't list them yet."""
    try:
        return filter_jobs(plugin.jobs(ctx), only)
    except (PluginError, OSError):
        return []


def _eta(estimates: stats.Estimates, phase: str, state: dict, waiting: list, running: list[dict], parallel: int,
         now: float, memory: dict[str, int], budget: float):
    """Return the seconds left; while preparing, the rest of prepare plus the jobs of the last matching run."""
    if phase == "preparing":
        prepare = estimates.prepare_duration() or 0.0
        left = max(prepare - (now - state.get("since", {}).get("preparing", now)), 0.0)
        return stats.eta(estimates, waiting or estimates.last_run_jobs, [], parallel, left, memory, budget)
    if phase == "running":
        active = [(item["job"], item["group"], item["share"], item["elapsed"]) for item in running]
        return stats.eta(estimates, waiting, active, parallel, memory=memory, budget=budget)
    return None


def write_progress(plugin: Plugin, ctx: RunContext, files: RunFiles) -> dict:
    data = snapshot(plugin, ctx, files)
    write_json(files.progress, data)
    return data
