# SPDX-License-Identifier: Apache-2.0
"""OpenSearch plugin stubs. README.md lists the intended design."""

from __future__ import annotations

from pathlib import Path

from cassandra_remote_ci.shared.plugin import Job, Plugin, RunContext


def _not_implemented(method: str) -> NotImplementedError:
    return NotImplementedError(f"opensearch plugin: {method} is not implemented")


class OpenSearchPlugin(Plugin):
    name = "opensearch"
    default_parallel = 1

    def detect(self, src: Path) -> bool:
        return False

    def default_jdk(self, src: Path) -> str:
        raise _not_implemented("default_jdk")

    def jobs(self, ctx: RunContext) -> list[Job]:
        raise _not_implemented("jobs")

    def run_job(self, ctx: RunContext, slot_dir: Path, job: Job, job_dir: Path, classes: list[str] | None = None) -> int:
        raise _not_implemented("run_job")

    def collect_results(self, ctx: RunContext, slot_dir: Path, job: Job, job_dir: Path) -> None:
        raise _not_implemented("collect_results")


PLUGIN = OpenSearchPlugin
