# SPDX-License-Identifier: Apache-2.0
"""ssh and rsync command lines over one shared connection, and the runners that execute or print them."""

from __future__ import annotations

import re
import shlex
import subprocess
import sys
from collections.abc import Sequence
from dataclasses import dataclass

SSH_OPTIONS = [
    "-o",
    "ControlMaster=auto",
    "-o",
    "ControlPath=~/.ssh/cassandra-remote-ci-%r@%h:%p",
    "-o",
    "ControlPersist=10m",
]
PROGRESS_OPTIONS = ["--info=progress2", "--no-inc-recursive"]


class CommandError(Exception):
    def __init__(self, cmd: list[str], returncode: int):
        super().__init__(f"command failed with exit code {returncode}: {shlex.join(cmd)}")
        self.returncode = returncode


@dataclass(frozen=True)
class Result:
    returncode: int
    stdout: str = ""


class CommandRunner:
    """Runs every external command of the CLI."""

    dry = False

    def run(self, cmd: list[str], *, input: str | None = None, capture: bool = False, check: bool = True) -> Result:
        completed = subprocess.run(cmd, input=input, text=True, stdout=subprocess.PIPE if capture else None)
        if check and completed.returncode != 0:
            raise CommandError(cmd, completed.returncode)
        return Result(completed.returncode, completed.stdout or "")


class DryRunRunner(CommandRunner):
    """Prints each command instead of running it and reports success."""

    dry = True

    def run(self, cmd: list[str], *, input: str | None = None, capture: bool = False, check: bool = True) -> Result:
        print(f"would run: {shlex.join(cmd)}", file=sys.stdout, flush=True)
        return Result(0, "")


def ssh_command(host: str, remote_command: str, tty: bool = False) -> list[str]:
    return ["ssh", *SSH_OPTIONS, *(["-t"] if tty else []), host, remote_command]


def rsync_progress_options(runner: CommandRunner) -> list[str]:
    """Return rsync options for one overall progress line, if stdout is a terminal and rsync is 3.1 or newer."""
    if runner.dry or not sys.stdout.isatty():
        return []
    match = re.search(r"version (\d+)\.(\d+)", runner.run(["rsync", "--version"], capture=True, check=False).stdout)
    return PROGRESS_OPTIONS if match and (int(match[1]), int(match[2])) >= (3, 1) else []


def rsync_command(
    sources: list[str],
    destination: str,
    backup_dir: str | None = None,
    excludes: Sequence[str] = (),
    options: Sequence[str] = (),
) -> list[str]:
    """Build an rsync over the shared ssh connection; with backup_dir, mirror and move replaced files there."""
    cmd = ["rsync", "-a", "-e", shlex.join(["ssh", *SSH_OPTIONS])]
    if backup_dir:
        cmd += ["--delete", "--backup", f"--backup-dir={backup_dir}"]
    for pattern in excludes:
        cmd += ["--exclude", pattern]
    return [*cmd, *options, *sources, destination]
