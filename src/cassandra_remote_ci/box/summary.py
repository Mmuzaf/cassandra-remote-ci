# SPDX-License-Identifier: Apache-2.0
"""Failure lists and the run summary: failures.*, crashed-jobs.txt, summary.md and summary.json."""

from __future__ import annotations

import time
from pathlib import Path

from cassandra_remote_ci.shared import junit
from cassandra_remote_ci.shared.plugin import Job, Plugin, RunContext
from cassandra_remote_ci.shared.state import PART_PREFIX, RunFiles, job_record, job_start, part_dirs, read_json, slot_path, write_json
from cassandra_remote_ci.shared.units import duration, gigabytes

SUMMARY_FAILURE_LIMIT = 200
CAUSE_WIDTH = 300


def collect_failures(
    files: RunFiles, jobs: list[Job], plugin: Plugin | None = None, ctx: RunContext | None = None
) -> tuple[list[dict], list[dict]]:
    """Return failing tests and crashed jobs; a plugin gives the crash causes, and with ctx the failures of running
    jobs."""
    failures, crashed = [], []
    for job in jobs:
        job_dir = files.job_dir(job.id)
        record = job_record(job_dir)
        log = str(job_dir / f"{PART_PREFIX}*" / "console.log" if record and record.get("parts") else job_dir / "console.log")
        if record is None:
            for work_dir in [job_dir, *part_dirs(job_dir)] if plugin and ctx else []:
                started = job_start(work_dir)
                work_log = str(work_dir / "console.log")
                if (work_dir / "part.json").exists():
                    found = [junit.TestResult(**data) for data in read_json(work_dir / "failures.json", [])]
                    failures += [_failure(job, result, work_log) for result in found]
                elif started:
                    results = junit.parse_results(plugin.in_progress_results(slot_path(ctx, started["slot"]),
                                                                             started["start"]))
                    failures += [_failure(job, result, work_log, running=True) for result in junit.failing(results)]
            continue
        job_failures = [junit.TestResult(**data) for data in read_json(job_dir / "failures.json", [])]
        failures += [_failure(job, result, log) for result in job_failures]
        if record["exit_code"] != 0 and not job_failures:
            crash_log = _crash_log(job_dir)
            lines = crash_log.read_text(errors="replace").splitlines() if crash_log.is_file() else []
            cause = plugin.crash_cause(lines)[:CAUSE_WIDTH] if plugin else ""
            crashed.append({"job": job.id, "group": job.group, "exit_code": record["exit_code"], "cause": cause,
                            "log": str(crash_log)})
    return failures, crashed


def _crash_log(job_dir: Path) -> Path:
    """Return the console log of the first part that exited non-zero, or the job's own log."""
    failed = [part_dir for part_dir in part_dirs(job_dir) if read_json(part_dir / "part.json", {}).get("exit_code")]
    return (failed[0] if failed else job_dir) / "console.log"


def _failure(job: Job, result: junit.TestResult, log: str, running: bool = False) -> dict:
    return {
        "group": job.group,
        "job": job.id,
        "test": result.test,
        "status": result.status,
        "message": result.message,
        "log": log,
        "running": running,
    }


def render_failures(failures: list[dict], crashed: list[dict]) -> str:
    lines = [f"{item['group']}\t{item['test']}\t{item['status']}\t{item['job']}\t{item['log']}" for item in failures]
    lines += [f"crashed\t{_crash_line(item)}" for item in crashed]
    return "\n".join(lines)


def _crash_line(item: dict) -> str:
    return f"{item['job']}\texit {item['exit_code']}\t{item.get('cause', '')}\t{item['log']}"


def write_failures(files: RunFiles, failures: list[dict], crashed: list[dict]) -> None:
    files.failures_txt.write_text(render_failures(failures, []) + ("\n" if failures else ""))
    write_json(files.failures_json, failures)
    files.crashed.write_text("".join(_crash_line(item) + "\n" for item in crashed))


def usage_table(usage: dict, budget: int | None) -> list[str]:
    """Return how busy the box's CPUs and memory were while jobs ran, and how much of the memory budget the jobs
    reserved, as far as usage.json knows."""
    if usage.get("cpu_average") is None:
        return []

    def percent(key: str) -> str:
        return f"{usage[key]:.0%}" if usage.get(key) is not None else "-"

    def size(key: str) -> str:
        return gigabytes(usage[key]) if usage.get(key) else "-"

    cpus = f"{usage['cpus']} CPUs" if usage.get("cpus") else "-"
    lines = [
        "",
        "| box while jobs ran | average | peak | total |",
        "|---|---|---|---|",
        f"| CPU | {percent('cpu_average')} | {percent('cpu_peak')} | {cpus} |",
        f"| memory | {size('memory_average')} | {size('memory_peak')} | {size('memory_total')} |",
    ]
    if usage.get("reserved_average") is not None:
        lines.append(f"| reserved | {size('reserved_average')} | {size('reserved_peak')} | "
                     f"{gigabytes(budget) if budget else '-'} |")
    return lines


def write_summary(files: RunFiles, snapshot: dict, failures: list[dict], crashed: list[dict]) -> None:
    info = read_json(files.info, {})
    write_json(
        files.summary_json,
        {"run": info, "progress": snapshot, "failures": failures, "crashed": crashed, "written": time.time()},
    )
    lines = [
        f"# {snapshot['run_id']}",
        "",
        f"Plugin {info.get('plugin')}, profile {info.get('profile') or '-'}, JDK {info.get('jdk')}, "
        f"{info.get('parallel')} slots of {info.get('memory')}, git {info.get('git_sha') or '-'}"
        f"{' with uncommitted changes' if info.get('dirty') else ''}.",
        f"Phase {snapshot['phase']}, elapsed {duration(snapshot['elapsed'])}.",
    ]
    lines += usage_table(read_json(files.usage, {}), info.get("memory_budget"))
    lines += [
        "",
        "| group | jobs | classes | tests | failures | errors | skipped | memory | peak memory |",
        "|---|---|---|---|---|---|---|---|---|",
    ]
    for row in snapshot["groups"]:
        memory = [gigabytes(row[key]) if row.get(key) else "-" for key in ("memory", "peak_memory")]
        lines.append(
            f"| {row['group']} | {row['jobs']}/{row['jobs_total']} | {row['classes']} | {row['tests']} "
            f"| {row['failures']} | {row['errors']} | {row['skipped']} | {memory[0]} | {memory[1]} |"
        )
    lines += ["", f"## Failures ({len(failures)})", ""]
    lines += [f"- {item['group']} `{item['test']}` {item['status']} in {item['job']}" for item in failures[:SUMMARY_FAILURE_LIMIT]]
    if len(failures) > SUMMARY_FAILURE_LIMIT:
        lines.append(f"- {len(failures) - SUMMARY_FAILURE_LIMIT} more in failures.txt")
    lines += ["", f"## Crashed jobs ({len(crashed)})", ""]
    lines += [f"- {item['job']} exit {item['exit_code']}" + (f": `{item['cause']}`" if item.get("cause") else "")
              for item in crashed]
    files.summary_md.write_text("\n".join(lines) + "\n")
