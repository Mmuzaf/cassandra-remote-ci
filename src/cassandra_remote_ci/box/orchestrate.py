# SPDX-License-Identifier: Apache-2.0
"""Runs on the box inside the systemd unit: prepare, slots, the job queue, and aggregation."""

from __future__ import annotations

import math
import os
import signal
import subprocess
import threading
import time
import traceback
from collections import Counter
from concurrent.futures import ThreadPoolExecutor
from dataclasses import asdict, dataclass
from pathlib import Path

from cassandra_remote_ci.box import progress, stats, summary
from cassandra_remote_ci.box.usage import Sampler
from cassandra_remote_ci.shared import junit
from cassandra_remote_ci.shared.plugin import Job, Plugin, PluginError, RunContext, Totals, filter_jobs
from cassandra_remote_ci.shared.state import (
    DEFAULT_MIN_CLASSES_PER_PART,
    PART_PREFIX,
    RunFiles,
    active_run,
    attempt_number,
    job_record,
    job_start,
    move_to_attempt,
    part_dirs,
    read_json,
    release_lock,
    set_phase,
    slot_path,
    write_json,
    write_lock,
)
from cassandra_remote_ci.shared.units import duration

PARTS_PER_SLOT = 3
BUDGET_WAIT = 5


def log(message: str) -> None:
    print(f"{time.strftime('%H:%M:%S')} {message}", flush=True)


def stop_containers(files: RunFiles) -> list[str]:
    """Run docker stop on every recorded container of the run that is still running, and return their IDs.

    A finished job's container can outlive it when the job's script is killed before its own docker stop.
    Plugins record containers under jobs/<id>/ and, for prepare, under prepare/.
    """
    recorded = []
    for containers in sorted(files.run_dir.rglob("containers")):
        recorded += [line.strip() for line in containers.read_text().splitlines() if line.strip()]
    if not recorded:
        return []
    filters = [f"--filter=id={container_id}" for container_id in recorded]
    running = subprocess.run(["docker", "ps", "-q", "--no-trunc", *filters], capture_output=True, text=True).stdout.split()
    if running:
        subprocess.run(["docker", "stop", "-t", "10", *running], capture_output=True)
    return running


@dataclass
class QueuedJob:
    job: Job
    remaining: list[str] | None
    parts: int = 0
    running: int = 0


def part_size(job_left: int, all_left: int, parallel: int, minimum: int) -> int:
    """Return how many of a job's classes a free slot takes.

    A third of a slot's even share of the classes left in the queue, at least minimum, so slots that finish early
    still find work. The rest of the job is cut into equal parts of at least that size.
    """
    size = max(minimum, math.ceil(all_left / (parallel * PARTS_PER_SLOT)))
    return math.ceil(job_left / max(1, job_left // size))


def write_counts(work_dir: Path) -> tuple[junit.Counts, list[junit.TestResult]]:
    results = junit.parse_results(junit.find_xml(work_dir / "results"))
    counts, failures = junit.count(results), junit.failing(results)
    write_json(work_dir / "counts.json", counts.to_dict())
    write_json(work_dir / "failures.json", [asdict(result) for result in failures])
    return counts, failures


class Orchestrator:
    def __init__(self, plugin: Plugin, ctx: RunContext, only: str | None = None):
        self.plugin = plugin
        self.ctx = ctx
        self.only = only
        self.files = RunFiles(ctx.run_dir)
        self.info = read_json(self.files.info, {})
        self.min_classes = int(self.info.get("min_classes_per_part") or DEFAULT_MIN_CLASSES_PER_PART)
        self.aborting = False
        self.usage = Sampler(self.files.usage)

    def run(self, retry: bool = False, include_failed: bool = False) -> int:
        """Run every job of the run, or with retry the crashed and never-run ones, then aggregate; return the exit code."""
        base_dir = self.files.base_dir
        holder = active_run(base_dir)
        if holder and holder != self.ctx.run_id:
            log(f"remote lock held by run {holder}; use 'abort' or wait")
            return 1
        write_lock(base_dir, self.ctx.run_id)
        stats.create(base_dir, self.ctx.run_id)
        previous_handler = signal.signal(signal.SIGTERM, self.on_sigterm)
        try:
            if retry:
                jobs = [Job.from_dict(data) for data in read_json(self.files.jobs, [])]
                pending = self.select_retry(jobs, include_failed)
                log(f"retrying {len(pending)} of {len(jobs)} jobs")
            else:
                jobs = self.plan()
                pending = jobs
            sampling = threading.Event()
            threading.Thread(target=self.usage.run, args=(sampling, log), daemon=True).start()
            try:
                first = None if retry else self.plugin.first_job(jobs)
                if first:
                    set_phase(self.files, "running")
                    log(f"running {first.id} alone first")
                    self.run_jobs([first], parallel=1)
                set_phase(self.files, "running")
                self.run_jobs(pending, parallel=self.ctx.parallel)
            finally:
                sampling.set()
            failures, crashed = self.aggregate(jobs)
            set_phase(self.files, "done")
            summary.write_summary(self.files, progress.write_progress(self.plugin, self.ctx, self.files), failures, crashed)
            log("done")
            return 0
        except Exception as error:
            log(traceback.format_exc())
            set_phase(self.files, "failed", error=f"{type(error).__name__}: {error}")
            return 1
        finally:
            signal.signal(signal.SIGTERM, previous_handler)
            release_lock(base_dir, self.ctx.run_id)

    def plan(self) -> list[Job]:
        set_phase(self.files, "preparing")
        log("prepare")
        self.plugin.prepare(self.ctx)
        for slot in range(1, self.ctx.parallel + 1):
            log(f"set up slot{slot}")
            self.plugin.setup_slot(self.ctx, slot_path(self.ctx, slot))
        started = read_json(self.files.state, {}).get("since", {}).get("preparing", time.time())
        stats.append(self.files.base_dir, stats.prepare_stats(self.info, started, time.time()))
        jobs = filter_jobs(self.plugin.jobs(self.ctx), self.only)
        if not jobs:
            raise PluginError(f"no jobs match --only {self.only!r}")
        write_json(self.files.jobs, [job.to_dict() for job in jobs])
        totals = self.plugin.totals(self.ctx, jobs) or Totals()
        totals.jobs_per_group = dict(Counter(job.group for job in jobs))
        write_json(self.files.totals, asdict(totals))
        log(f"{len(jobs)} jobs in {len(totals.jobs_per_group)} groups")
        return jobs

    def select_retry(self, jobs: list[Job], include_failed: bool) -> list[Job]:
        """Pick crashed and never-run jobs, plus failed ones with include_failed, and move their earlier files aside."""
        picked = []
        for job in jobs:
            job_dir = self.files.job_dir(job.id)
            record = job_record(job_dir)
            failures = read_json(job_dir / "failures.json", [])
            crashed = record is not None and record["exit_code"] != 0 and not failures
            failed = record is not None and (bool(failures) or record["exit_code"] != 0)
            if record is None or crashed or (include_failed and failed):
                if job_dir.is_dir():
                    move_to_attempt(job_dir)
                picked.append(job)
        return picked

    def run_jobs(self, jobs: list[Job], parallel: int) -> None:
        """Run jobs on parallel slots within the memory budget of the run.

        A free slot takes the first queued job whose memory fits what the running jobs leave of the budget, whole,
        or a part of its classes if it has them. A job bigger than the budget runs once nothing else does.
        """
        queue = [
            QueuedJob(job, list(self.plugin.test_classes(self.ctx, job)) or None)
            for job in jobs
            if job_record(self.files.job_dir(job.id)) is None
        ]
        memory = {job.id: self.plugin.job_memory(self.ctx, job) for job in jobs}
        budget = self.info.get("memory_budget") or math.inf
        reserved = 0
        changed = threading.Condition()

        def take() -> tuple[QueuedJob, int, list[str] | None] | None:
            nonlocal reserved
            with changed:
                while queue and not self.aborting:
                    head = next((item for item in queue if not reserved or reserved + memory[item.job.id] <= budget), None)
                    if head is None:
                        changed.wait(BUDGET_WAIT)
                        continue
                    reserved += memory[head.job.id]
                    if head.remaining is None:
                        queue.remove(head)
                        return head, 0, None
                    left = sum(len(item.remaining) for item in queue if item.remaining)
                    size = part_size(len(head.remaining), left, parallel, self.min_classes)
                    part, head.remaining = head.remaining[:size], head.remaining[size:]
                    head.parts += 1
                    head.running += 1
                    if not head.remaining:
                        queue.remove(head)
                    job_dir = self.files.job_dir(head.job.id)
                    if head.parts == 1 and job_dir.is_dir() and any(job_dir.iterdir()):
                        move_to_attempt(job_dir)
                    return head, head.parts, part
                return None

        def work(slot: int) -> None:
            nonlocal reserved
            while not self.aborting and (taken := take()):
                item, number, part = taken
                try:
                    if part is None:
                        self.run_one(slot, item.job)
                    else:
                        self.run_part(slot, item.job, number, part)
                finally:
                    with changed:
                        reserved -= memory[item.job.id]
                        changed.notify_all()
                if part is None:
                    continue
                with changed:
                    item.running -= 1
                    last = item.running == 0 and not item.remaining
                if last and not self.aborting:
                    self.finish_parts(item)

        with ThreadPoolExecutor(max_workers=parallel) as pool:
            for future in [pool.submit(work, slot) for slot in range(1, parallel + 1)]:
                future.result()

    def execute(self, slot: int, job: Job, work_dir: Path, classes: list[str] | None) -> tuple[int, junit.Counts, list]:
        """Run a job or part in a slot, collect its results into work_dir, and write counts.json and failures.json."""
        slot_dir = slot_path(self.ctx, slot)
        self.usage.watch(work_dir, self.plugin.job_memory(self.ctx, job))
        try:
            exit_code = self.plugin.run_job(self.ctx, slot_dir, job, work_dir, classes)
        except Exception:
            exit_code = 255
            self.append_error(work_dir)
        finally:
            self.usage.unwatch(work_dir)
        try:
            self.plugin.collect_results(self.ctx, slot_dir, job, work_dir)
        except Exception:
            self.append_error(work_dir)
        counts, failures = write_counts(work_dir)
        return exit_code, counts, failures

    def run_one(self, slot: int, job: Job) -> None:
        job_dir = self.files.job_dir(job.id)
        job_dir.mkdir(parents=True, exist_ok=True)
        interrupted = job_start(job_dir)
        attempt = interrupted["attempt"] if interrupted else attempt_number(job_dir)
        start = time.time()
        write_json(job_dir / "started.json", {"slot": slot, "start": start, "attempt": attempt})
        log(f"start {job.id} in slot{slot}")
        exit_code, counts, failures = self.execute(slot, job, job_dir, None)
        if self.aborting:
            return
        end = time.time()
        record = {"id": job.id, "group": job.group, "slot": slot, "start": start, "end": end, "exit_code": exit_code,
                  "attempt": attempt, "peak_memory": self.usage.peak_memory(job_dir)}
        write_json(job_dir / "job.json", record)
        stats.append(self.files.base_dir, stats.job_stats(self.info, record, counts.tests, len(failures), end - start))
        log(
            f"end {job.id} exit {exit_code} in {duration(end - start)}, "
            f"{counts.tests} tests, {counts.failures} failures, {counts.errors} errors"
        )

    def run_part(self, slot: int, job: Job, number: int, classes: list[str]) -> None:
        part_dir = self.files.job_dir(job.id) / f"{PART_PREFIX}{number}"
        part_dir.mkdir(parents=True)
        start = time.time()
        write_json(part_dir / "started.json", {"slot": slot, "start": start, "classes": classes})
        log(f"start {job.id} {part_dir.name} with {len(classes)} classes in slot{slot}")
        exit_code, counts, _ = self.execute(slot, job, part_dir, classes)
        if self.aborting:
            return
        end = time.time()
        write_json(part_dir / "part.json", {"slot": slot, "start": start, "end": end, "exit_code": exit_code,
                                            "peak_memory": self.usage.peak_memory(part_dir)})
        log(
            f"end {job.id} {part_dir.name} exit {exit_code} in {duration(end - start)}, "
            f"{counts.tests} tests, {counts.failures} failures, {counts.errors} errors"
        )

    def finish_parts(self, item: QueuedJob) -> None:
        """Merge the results of a job's parts into the job directory and write its job.json."""
        job = item.job
        job_dir = self.files.job_dir(job.id)
        records = []
        for part_dir in part_dirs(job_dir):
            records.append(read_json(part_dir / "part.json"))
            results = part_dir / "results"
            for path in sorted(results.rglob("*")) if results.is_dir() else []:
                if path.is_file():
                    target = job_dir / "results" / path.relative_to(results)
                    target.parent.mkdir(parents=True, exist_ok=True)
                    path.replace(target)
        counts, failures = write_counts(job_dir)
        exit_code = next((record["exit_code"] for record in records if record["exit_code"] != 0), 0)
        start, end = min(record["start"] for record in records), max(record["end"] for record in records)
        peaks = [record["peak_memory"] for record in records if record.get("peak_memory")]
        record = {"id": job.id, "group": job.group, "start": start, "end": end, "exit_code": exit_code,
                  "attempt": attempt_number(job_dir), "parts": len(records), "peak_memory": max(peaks, default=None)}
        write_json(job_dir / "job.json", record)
        work = sum(part["end"] - part["start"] for part in records)
        stats.append(self.files.base_dir, stats.job_stats(self.info, record, counts.tests, len(failures), work))
        log(
            f"end {job.id} exit {exit_code} in {duration(end - start)} over {len(records)} parts, "
            f"{counts.tests} tests, {counts.failures} failures, {counts.errors} errors"
        )

    @staticmethod
    def append_error(job_dir: Path) -> None:
        with open(job_dir / "console.log", "a") as out:
            out.write(traceback.format_exc())

    def aggregate(self, jobs: list[Job]) -> tuple[list[dict], list[dict]]:
        """Write the failure lists and the plugin's report, and return the failures and crashed jobs."""
        set_phase(self.files, "aggregating")
        log("aggregate")
        failures, crashed = summary.collect_failures(self.files, jobs, self.plugin)
        summary.write_failures(self.files, failures, crashed)
        try:
            self.plugin.report(self.ctx, crashed)
        except Exception:
            log("report failed\n" + traceback.format_exc())
        return failures, crashed

    def record_interrupted(self) -> None:
        """Add the jobs still running to the stats as interrupted, so their time so far bounds later ETAs."""
        now = time.time()
        for data in read_json(self.files.jobs, []):
            job_dir = self.files.job_dir(data["id"])
            if job_record(job_dir) is not None:
                continue
            spans = []
            for work_dir in [job_dir, *part_dirs(job_dir)]:
                started = job_start(work_dir)
                if started:
                    spans.append((started["start"], (read_json(work_dir / "part.json") or {}).get("end", now)))
            if spans:
                work = sum(end - start for start, end in spans)
                record = stats.interrupted_stats(self.info, data["id"], data["group"], min(spans)[0], now, work)
                stats.append(self.files.base_dir, record)

    def on_sigterm(self, signum, frame) -> None:
        """Stop the containers, mark the run aborted and exit.

        Must not take locks before ignoring SIGTERM: a second SIGTERM, e.g. forwarded by uv, runs this handler
        again on top of itself, and a nested handler waiting for a lock its outer call holds never returns.
        """
        signal.signal(signal.SIGTERM, signal.SIG_IGN)
        if self.aborting:
            return
        self.aborting = True
        log("got SIGTERM; stopping containers")
        stopped = stop_containers(self.files)
        log(f"stopped {len(stopped)} containers")
        self.record_interrupted()
        set_phase(self.files, "aborted")
        release_lock(self.files.base_dir, self.ctx.run_id)
        os._exit(143)
