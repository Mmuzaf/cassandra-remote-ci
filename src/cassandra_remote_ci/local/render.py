# SPDX-License-Identifier: Apache-2.0
"""The text that status prints, from the snapshot the box returns as JSON."""

from __future__ import annotations

from cassandra_remote_ci.shared.state import FINISHED_PHASES
from cassandra_remote_ci.shared.units import duration, gigabytes

INTERRUPTED_UNIT_STATES = ("inactive", "failed")


def _of(done: int, total: int | None, estimate: int | None = None) -> str:
    if total is not None:
        return f"{done:,}/{total:,}"
    if estimate:
        return f"{done:,}/~{estimate:,}"
    return f"{done:,}"


def table(headers: list[str], rows: list[list[str]], right: set[int], total: list[str] | None = None) -> list[str]:
    """Format rows in columns under headers and = rules; right holds the right-aligned columns, total goes under - rules."""
    widths = [max(len(row[column]) for row in [headers, *rows, *([total] if total else [])]) for column in range(len(headers))]

    def line(cells: list[str]) -> str:
        padded = [cell.rjust(width) if column in right else cell.ljust(width) for column, (cell, width) in enumerate(zip(cells, widths))]
        return "  ".join(padded).rstrip()

    lines = [line(headers), "  ".join("=" * width for width in widths), *map(line, rows)]
    if total:
        lines += ["  ".join("-" * width for width in widths), line(total)]
    return lines


def _left(item: dict) -> str:
    """Return the expected time left of a running job, with + once it runs over."""
    if item.get("expected") is None:
        return "-"
    left = item["expected"] - item["elapsed"]
    return f"~{duration(left)}" if left >= 0 else f"+{duration(-left)}"


def box_line(data: dict) -> str:
    """Return how busy the box's CPUs are and how much of its memory the jobs use and reserve, as far as known."""
    usage = data.get("usage") or {}
    parts = []
    if usage.get("cpu") is not None:
        parts.append(f"CPU {usage['cpu']:.0%} busy, {usage['cpu_average']:.0%} on average")
    if usage.get("memory_total"):
        parts.append(f"memory {gigabytes(usage['memory_used'])} of {gigabytes(usage['memory_total'])} used")
    if data.get("memory_budget"):
        parts.append(f"jobs reserve {gigabytes(data['memory_reserved'])} of the {gigabytes(data['memory_budget'])} budget")
    return f"box: {', '.join(parts)}" if parts else ""


def _block(title: str, rows: list[tuple[str, str]]) -> list[str]:
    width = max(len(label) for label, _ in rows)
    return ["", title, *[f"  {f'{label:<{width}}  ' if width else ''}{value}".rstrip() for label, value in rows]]


def render(data: dict, unit: str | None = None) -> str:
    """Format a snapshot as the text that status prints."""
    rows = data["groups"]
    jobs = data["jobs"]
    live = data["phase"] not in FINISHED_PHASES
    interrupted = live and unit in INTERRUPTED_UNIT_STATES
    summary = ["interrupted" if interrupted else data["phase"], f"elapsed {duration(data['elapsed'])}"]
    if data.get("eta") and live and not interrupted:
        summary.append(f"eta ~{duration(data['eta'])}")
    summary += [f"{jobs['done']} of {jobs['total']} jobs done"] if jobs["total"] else []
    summary += [f"{len(jobs['running'])} {'interrupted' if interrupted else 'running'}"] if jobs["running"] else []
    summary += [f"{jobs['crashed']} crashed"] if jobs["crashed"] else []
    summary += [f"unit {unit}"] if unit else []
    header = f"{data['run_id']}: {', '.join(summary)}"
    lines = [header, "-" * len(header)]
    if data["phase"] == "running" and not interrupted:
        lines += [line for line in [box_line(data)] if line]

    if interrupted:
        lines += _block("Interrupted", [("", "the run's unit stopped before the run finished, e.g. on a reboot; "
                                             "'retry' runs the interrupted and remaining jobs")])
    if data.get("error"):
        lines += _block("Error", [("", data["error"])])

    if jobs["running"]:
        running = []
        for item in sorted(jobs["running"], key=lambda item: item["slot"]):
            part = [str(item["part"]), str(item["classes"])] if item.get("part") else ["-", "-"]
            job = [str(item["slot"]), item["job"], *part, duration(item["elapsed"]), _left(item)]
            containers = item.get("containers") or [{}]
            for index, container in enumerate(containers):
                cells = [container.get("id") or "-", container.get("status") or "-", container.get("cpu") or "-",
                         container.get("mem") or "-"]
                running.append((job if index == 0 else [""] * len(job)) + cells)
        headers = ["SLOT", "JOB", "PART", "CLASSES", "ELAPSED", "LEFT", "CONTAINER", "STATE", "CPU", "MEM"]
        lines += ["", *table(headers, running, right={0, 2, 3, 4, 5, 8, 9})]

    if rows:
        cells = [
            [row["group"], _of(row["jobs"], row["jobs_total"]), _of(row["classes"], row["classes_total"]),
             _of(row["tests"], row["tests_total"], row["tests_estimate"]), str(row["failures"]), str(row["errors"]),
             str(row["skipped"])]
            for row in rows
        ]
        counted = [row for row in rows if row["classes_total"] is not None] or rows
        class_totals = [row["classes_total"] for row in counted if row["classes_total"] is not None]
        expected = [row["tests_total"] or row["tests_estimate"] for row in rows]
        exact = all(row["tests_total"] for row in rows)
        tests = sum(row["tests"] for row in rows)
        total = [
            "total",
            _of(jobs["done"], jobs["total"]),
            _of(sum(row["classes"] for row in counted), sum(class_totals) if class_totals else None),
            _of(tests, sum(expected) if exact else None, sum(expected) if all(expected) else None),
            *[str(sum(row[key] for row in rows)) for key in ("failures", "errors", "skipped")],
        ]
        headers = ["GROUP", "JOBS", "CLASSES", "TESTS", "FAIL", "ERR", "SKIP"]
        lines += ["", *table(headers, cells, right={1, 2, 3, 4, 5, 6}, total=total)]

    if data.get("recent"):
        recent = [[item["test"], item["status"], item["group"], item["job"]] for item in data["recent"]]
        lines += ["", *table(["RECENT_FAILURE", "STATUS", "GROUP", "JOB"], recent, right=set())]

    activity = [
        (f"slot{item['slot']}" if index == 0 else "", line)
        for item in sorted(jobs["running"], key=lambda item: item["slot"])
        for index, line in enumerate(item.get("activity", []))
    ]
    if activity:
        lines += _block("Activity", activity)
    if data.get("log"):
        lines += _block("Log", [("", data["log"])])
    return "\n".join(lines)
