# SPDX-License-Identifier: Apache-2.0
"""Memory and CPU limits in a slot's copy of .build/docker/run-tests.sh."""

from __future__ import annotations

from pathlib import Path

from cassandra_remote_ci.shared.plugin import PluginError

MEMORY_FLAGS = ("-m 5g --memory-swap 5g", "-m 15g --memory-swap 15g")
CPUS_LINE = 'docker_cpus=$(echo "scale=2; ${cores} / ( ${jenkins_executors} )" | bc)'


def patch(script: Path, memory: str) -> None:
    """Let a docker_cpus variable in the environment set each container's CPUs, and with memory, give every
    container that memory instead of the per-target limits of run-tests.sh."""
    text = script.read_text()
    missing = [pattern for pattern in (CPUS_LINE, *(MEMORY_FLAGS if memory else ())) if pattern not in text]
    if missing:
        raise PluginError(f"{script} has changed; can't find {', '.join(repr(pattern) for pattern in missing)}")
    for flags in MEMORY_FLAGS if memory else ():
        text = text.replace(flags, f"-m {memory} --memory-swap {memory}")
    script.write_text(text.replace(CPUS_LINE, f'[ -n "${{docker_cpus}}" ] || {CPUS_LINE}'))
