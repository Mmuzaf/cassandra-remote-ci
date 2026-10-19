# SPDX-License-Identifier: Apache-2.0
"""The box's CPU and memory use while a run's jobs run, and the peak memory of each job's containers."""

from __future__ import annotations

import os
import re
import subprocess
import threading
import time
import traceback
from pathlib import Path
from typing import Callable

from cassandra_remote_ci.shared.state import read_json, write_json

SAMPLE_INTERVAL = 30
MEMORY_UNITS = {"B": 1, "kB": 10**3, "MB": 10**6, "GB": 10**9, "KiB": 2**10, "MiB": 2**20, "GiB": 2**30, "TiB": 2**40}


def cpu_times() -> tuple[int, int] | None:
    """Return the busy and total jiffies of all CPUs from /proc/stat, or None where there is none."""
    try:
        fields = [int(value) for value in Path("/proc/stat").read_text().split("\n", 1)[0].split()[1:9]]
    except (OSError, ValueError):
        return None
    return sum(fields) - fields[3] - fields[4], sum(fields)


def memory_use() -> dict:
    """Return the used and total bytes of memory from /proc/meminfo, used being what MemAvailable doesn't count."""
    try:
        values = dict(line.split(":", 1) for line in Path("/proc/meminfo").read_text().splitlines() if ":" in line)
        total, available = (int(values[key].split()[0]) * 1024 for key in ("MemTotal", "MemAvailable"))
    except (OSError, KeyError, ValueError):
        return {}
    return {"memory_used": total - available, "memory_total": total}


def memory_bytes(text: str) -> int:
    """Return the bytes in a docker stats size such as 1.5GiB, 0 if it can't be read."""
    match = re.fullmatch(r"([\d.]+)\s*([kKMGT]?i?B)", text.strip())
    return int(float(match.group(1)) * MEMORY_UNITS.get(match.group(2), 0)) if match else 0


def add_sample(usage: dict, busy: float | None, memory: dict, reserved: int, now: float) -> dict:
    """Return usage with the current CPU busy fraction and memory use, and with them and the memory the running jobs
    reserve added to its averages and peaks.

    A sample missing the CPU or memory use only updates the current values, so averages cover the same samples.
    """
    usage = {**usage, "updated": now, **memory}
    if busy is not None:
        usage["cpu"] = busy
    if busy is None or not memory:
        return usage
    samples = usage.get("samples", 0)

    def average(key: str, value: float) -> float:
        return (usage.get(key, 0) * samples + value) / (samples + 1)

    used = memory["memory_used"]
    return {
        **usage,
        "cpu_average": average("cpu_average", busy),
        "cpu_peak": max(usage.get("cpu_peak", 0.0), busy),
        "memory_average": round(average("memory_average", used)),
        "memory_peak": max(usage.get("memory_peak", 0), used),
        "reserved_average": round(average("reserved_average", reserved)),
        "reserved_peak": max(usage.get("reserved_peak", 0), reserved),
        "samples": samples + 1,
    }


class Sampler:
    """Writes the box's CPU and memory use to a usage.json, and keeps the highest memory use of the containers of
    each watched job or part, keyed by its work directory."""

    def __init__(self, path: Path):
        self.path = path
        self.lock = threading.Lock()
        self.watched: dict[Path, int] = {}
        self.peaks: dict[Path, int] = {}

    def watch(self, work_dir: Path, memory: int) -> None:
        """Sample the containers of a job or part that reserves memory bytes."""
        with self.lock:
            self.watched[work_dir] = memory

    def unwatch(self, work_dir: Path) -> None:
        with self.lock:
            self.watched.pop(work_dir, None)

    def reserved(self) -> int:
        with self.lock:
            return sum(self.watched.values())

    def peak_memory(self, work_dir: Path) -> int | None:
        """Return and forget the highest memory use sampled in the containers of a finished job or part."""
        with self.lock:
            return self.peaks.pop(work_dir, None)

    def run(self, stop: threading.Event, log: Callable[[str], None]) -> None:
        """Sample every SAMPLE_INTERVAL seconds until stop is set, adding to the averages of earlier attempts."""
        previous, usage = cpu_times(), {**read_json(self.path, {}), "cpus": os.cpu_count()}
        while not stop.wait(SAMPLE_INTERVAL):
            try:
                current, busy = cpu_times(), None
                if previous and current and current[1] > previous[1]:
                    busy = (current[0] - previous[0]) / (current[1] - previous[1])
                previous = current
                usage = add_sample(usage, busy, memory_use(), self.reserved(), time.time())
                write_json(self.path, usage)
                self.sample_containers()
            except Exception:
                log("usage sample failed\n" + traceback.format_exc())

    def sample_containers(self) -> None:
        with self.lock:
            work_dirs = list(self.watched)
        owners = {}
        for work_dir in work_dirs:
            containers = work_dir / "containers"
            lines = containers.read_text().splitlines() if containers.exists() else []
            owners.update((line.strip(), work_dir) for line in lines if line.strip())
        if not owners:
            return
        cmd = ["docker", "stats", "--no-stream", "--no-trunc", "--format", "{{.ID}} {{.MemUsage}}", *owners]
        output = subprocess.run(cmd, capture_output=True, text=True, timeout=60).stdout
        for line in output.splitlines():
            full_id, _, usage = line.partition(" ")
            work_dir = next((owner for prefix, owner in owners.items() if full_id.startswith(prefix)), None)
            if work_dir:
                with self.lock:
                    self.peaks[work_dir] = max(self.peaks.get(work_dir, 0), memory_bytes(usage.split("/")[0]))
