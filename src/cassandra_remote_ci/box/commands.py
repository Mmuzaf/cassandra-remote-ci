# SPDX-License-Identifier: Apache-2.0
"""Commands the CLI runs on the box with uv run python -m cassandra_remote_ci.box.commands."""

from __future__ import annotations

import argparse
import json
import subprocess
import sys
import time
from pathlib import Path

from cassandra_remote_ci.box import progress, summary
from cassandra_remote_ci.box.orchestrate import Orchestrator, stop_containers
from cassandra_remote_ci.shared.plugin import Job, Plugin, PluginError, load_plugin
from cassandra_remote_ci.shared.state import (
    FINISHED_EXIT,
    FINISHED_PHASES,
    PART_PREFIX,
    RunFiles,
    active_run,
    context_for,
    job_record,
    list_runs,
    phase_of,
    read_json,
    release_lock,
    resolve_run,
    run_files,
    set_phase,
    unit_name,
    unit_state,
    write_json,
)

FETCHED_FILES = (
    "run.json",
    "state.json",
    "jobs.json",
    "totals.json",
    "progress.json",
    "summary.md",
    "summary.json",
    "failures.txt",
    "failures.json",
    "crashed-jobs.txt",
    "usage.json",
    "report",
)


def prepare_run(base_dir: Path, run_id: str) -> int:
    """Write run.json from stdin for a new run, unless another run holds the lock."""
    holder = active_run(base_dir)
    if holder:
        raise PluginError(f"remote lock held by run {holder}; use 'abort' or wait")
    files = run_files(base_dir, run_id)
    if files.info.exists():
        raise PluginError(f"run {run_id} already exists")
    info = json.load(sys.stdin)
    write_json(files.info, info)
    set_phase(files, "queued")
    print(files.run_dir)
    return 0


def orchestrate(base_dir: Path, run_id: str, retry: bool, include_failed: bool) -> int:
    info = read_json(run_files(base_dir, run_id).info)
    plugin = load_plugin(info["plugin"])
    ctx = context_for(base_dir, info)
    return Orchestrator(plugin, ctx, info.get("only")).run(retry=retry, include_failed=include_failed)


def command_output(cmd: list[str]) -> str:
    """Return the stdout of cmd, or an empty string if it can't run, fails or takes over 30 seconds."""
    try:
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=30)
    except (OSError, subprocess.TimeoutExpired):
        return ""
    return result.stdout if result.returncode == 0 else ""


def json_lines(text: str) -> list[dict]:
    return [json.loads(line) for line in text.splitlines() if line.strip()]


def container_details(ids: list[str]) -> dict[str, dict]:
    """Return name, status, CPU and memory of the given containers, keyed by the IDs as given."""
    if not ids:
        return {}
    filters = [f"--filter=id={container_id}" for container_id in ids]
    listed = json_lines(command_output(["docker", "ps", "-a", "--no-trunc", "--format", "{{json .}}", *filters]))
    running = [row["ID"] for row in listed if row.get("State") == "running"]
    stats = {}
    if running:
        cmd = ["docker", "stats", "--no-stream", "--no-trunc", "--format", "{{json .}}", *running]
        stats = {row["ID"]: row for row in json_lines(command_output(cmd))}
    details = {}
    for container_id in ids:
        row = next((row for row in listed if row["ID"].startswith(container_id)), None)
        if row:
            usage = stats.get(row["ID"], {})
            details[container_id] = {
                "id": row["ID"][:12],
                "name": row.get("Names", ""),
                "status": row.get("Status", ""),
                "cpu": usage.get("CPUPerc"),
                "mem": usage.get("MemUsage"),
            }
    return details


def add_live_details(plugin: Plugin, files: RunFiles, data: dict, run_id: str) -> None:
    """Add the last orchestrator log line, and the containers and last console line of each running job."""
    journal = ["journalctl", "--user", "-u", unit_name(run_id), "-n", "1", "-o", "cat", "--no-pager"]
    data["log"] = progress.last_line(command_output(journal))
    running = data["jobs"]["running"]
    work_dirs = [
        files.job_dir(item["job"]) / f"{PART_PREFIX}{item['part']}" if item.get("part") else files.job_dir(item["job"])
        for item in running
    ]
    recorded = []
    for work_dir in work_dirs:
        containers = work_dir / "containers"
        lines = containers.read_text().splitlines() if containers.exists() else []
        recorded.append([line.strip() for line in lines if line.strip()])
    details = container_details([container_id for ids in recorded for container_id in ids])
    for item, work_dir, ids in zip(running, work_dirs, recorded):
        item["containers"] = [details[container_id] for container_id in ids if container_id in details]
        tail = progress.log_tail(work_dir / "console.log")
        item["activity"] = [line[: progress.LINE_WIDTH] for line in plugin.activity(tail)]


def status(base_dir: Path, run_id: str) -> int:
    """Print the progress of a run as JSON, which the CLI renders; exit with FINISHED_EXIT once it has stopped."""
    files = run_files(base_dir, run_id)
    info = read_json(files.info)
    plugin = load_plugin(info["plugin"])
    data = progress.write_progress(plugin, context_for(base_dir, info), files)
    data["unit"] = unit_state(run_id)
    if data["phase"] not in FINISHED_PHASES:
        add_live_details(plugin, files, data, run_id)
    print(json.dumps(data, indent=2))
    stopped = data["phase"] not in FINISHED_PHASES and data["unit"] in ("inactive", "failed")
    return FINISHED_EXIT if data["phase"] in FINISHED_PHASES or stopped else 0


def failures(base_dir: Path, run_id: str, as_json: bool) -> int:
    files = run_files(base_dir, run_id)
    info = read_json(files.info)
    jobs = [Job.from_dict(data) for data in read_json(files.jobs, [])]
    found, crashed = summary.collect_failures(
        files, jobs, load_plugin(info["plugin"]), context_for(base_dir, info)
    )
    if as_json:
        print(json.dumps({"run_id": run_id, "failures": found, "crashed": crashed}, indent=2))
    else:
        print(summary.render_failures(found, crashed) or f"no failures in run {run_id}")
    return 0


def stop(base_dir: Path, run_id: str) -> int:
    """Stop the recorded containers of a run, mark it aborted and release the lock."""
    files = run_files(base_dir, run_id)
    stopped = stop_containers(files)
    if phase_of(files) not in FINISHED_PHASES:
        set_phase(files, "aborted")
    release_lock(base_dir, run_id)
    if stopped:
        print(f"stopped {len(stopped)} containers of run {run_id}")
    else:
        print(f"no containers of run {run_id} are running")
    return 0


def fetch_list(base_dir: Path, run_id: str, all_logs: bool) -> int:
    """Print the paths, relative to the run directory, that fetch copies."""
    files = run_files(base_dir, run_id)
    paths = [name for name in FETCHED_FILES if (files.run_dir / name).exists()]
    for data in read_json(files.jobs, []):
        job_dir = files.job_dir(data["id"])
        record = job_record(job_dir)
        failed = record is None or record["exit_code"] != 0 or read_json(job_dir / "failures.json", [])
        if job_dir.is_dir() and (all_logs or failed):
            paths.append(f"jobs/{data['id']}")
    print("\n".join(paths))
    return 0


def list_old_runs(base_dir: Path, older_than: int) -> int:
    """Print runs whose run.json is older than older_than seconds, except the active run."""
    cutoff = time.time() - older_than
    active = active_run(base_dir)
    for run_id in list_runs(base_dir):
        if run_id != active and run_files(base_dir, run_id).info.stat().st_mtime < cutoff:
            print(run_id)
    return 0


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="python -m cassandra_remote_ci.box.commands")
    parser.add_argument("--dir", type=Path, default=Path.cwd(), help="remote directory")
    commands = parser.add_subparsers(dest="command", required=True)
    commands.add_parser("prepare-run").add_argument("run_id")
    command = commands.add_parser("orchestrate")
    command.add_argument("run_id")
    command.add_argument("--retry", action="store_true")
    command.add_argument("--failed", action="store_true")
    for name in ("status", "failures"):
        command = commands.add_parser(name)
        command.add_argument("run_id", nargs="?")
        command.add_argument("--json", action="store_true", help="status prints JSON either way")
    commands.add_parser("resolve").add_argument("run_id", nargs="?")
    commands.add_parser("stop-containers").add_argument("run_id")
    command = commands.add_parser("fetch-list")
    command.add_argument("run_id")
    command.add_argument("--all-logs", action="store_true")
    commands.add_parser("list-runs").add_argument("--older-than", type=int, default=0, help="seconds")
    args = parser.parse_args(argv)
    base_dir = args.dir.expanduser().resolve()

    try:
        match args.command:
            case "prepare-run":
                return prepare_run(base_dir, args.run_id)
            case "orchestrate":
                return orchestrate(base_dir, args.run_id, args.retry, args.failed)
            case "status":
                return status(base_dir, resolve_run(base_dir, args.run_id))
            case "failures":
                return failures(base_dir, resolve_run(base_dir, args.run_id), args.json)
            case "resolve":
                print(resolve_run(base_dir, args.run_id))
                return 0
            case "stop-containers":
                return stop(base_dir, args.run_id)
            case "fetch-list":
                return fetch_list(base_dir, args.run_id, args.all_logs)
            case "list-runs":
                return list_old_runs(base_dir, args.older_than)
    except (LookupError, PluginError) as error:
        print(f"error: {error}", file=sys.stderr)
        return 1
    return 2


if __name__ == "__main__":
    sys.exit(main())
