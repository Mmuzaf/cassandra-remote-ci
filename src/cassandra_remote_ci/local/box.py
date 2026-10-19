# SPDX-License-Identifier: Apache-2.0
"""ssh and rsync calls to the box: preflight checks, the box's commands, and the systemd unit of a run."""

from __future__ import annotations

import shlex
from collections.abc import Sequence
from pathlib import Path

from cassandra_remote_ci.local.ssh import CommandRunner, Result, rsync_command, rsync_progress_options, ssh_command
from cassandra_remote_ci.shared.state import unit_name

PROJECT_DIR = Path(__file__).resolve().parents[3]
BOX_MODULE = "cassandra_remote_ci.box.commands"
RUN_PLACEHOLDER = "RUN_ID"
STOP_TIMEOUT = "30s"

PREFLIGHT_SCRIPT = "; ".join(
    [
        'dir="$1"',
        "shift",
        'case "$dir" in "~") dir="$HOME" ;; "~/"*) dir="$HOME/${dir#"~/"}" ;; /*) ;; *) dir="$HOME/$dir" ;; esac',
        'echo "base=$dir"',
        'uv="$(command -v uv || true)"',
        'for candidate in "$HOME/.local/bin/uv" "$HOME/.cargo/bin/uv"; do [ -n "$uv" ] || { [ -x "$candidate" ] && uv="$candidate"; }; done',
        'echo "uv=$uv"',
        'for name in "$@"; do if command -v "$name" >/dev/null 2>&1; then echo "command $name=ok"; fi; done',
        'if docker info >/dev/null 2>&1; then echo "docker access=ok"; fi',
        'echo "docker memory=$(docker info --format {{.MemTotal}} 2>/dev/null)"',
        'echo "docker os=$(docker info --format {{.OperatingSystem}} 2>/dev/null)"',
        'if systemctl --user show-environment >/dev/null 2>&1; then echo "systemd user=ok"; fi',
        'if [ "$#" -gt 0 ] && systemd-run --user --wait --quiet --collect docker info >/dev/null 2>&1; then echo "docker in units=ok"; fi',
        'echo "linger=$(loginctl show-user "$(id -un)" -p Linger --value 2>/dev/null)"',
        'run="$(cat "$dir/lock" 2>/dev/null)"',
        'if [ -n "$run" ] && systemctl --user is-active --quiet "cassandra-remote-ci-$run"; then echo "active=$run"; fi',
        "true",
    ]
)


class UsageError(Exception):
    pass


class Remote:
    """ssh and rsync calls to the box, and paths in its remote directory."""

    def __init__(self, host: str, runner: CommandRunner, remote_dir: str):
        self.host = host
        self.runner = runner
        self.remote_dir = remote_dir
        self.base = ""
        self.uv = ""
        self.active = ""
        self.docker_memory = 0
        self.docker_os = ""
        self.progress: list[str] | None = None

    @property
    def ci_dir(self) -> str:
        return f"{self.base}/ci"

    def run_dir(self, run_id: str) -> str:
        return f"{self.base}/runs/{run_id}"

    def ssh(
        self, command: list[str], *, capture: bool = False, check: bool = True, input: str | None = None, tty: bool = False
    ) -> Result:
        return self.runner.run(ssh_command(self.host, shlex.join(command), tty), input=input, capture=capture, check=check)

    def preflight(self, commands: list[str] | None = None) -> None:
        """Find the remote directory, uv and the active run; with commands, also check the box can start runs."""
        script = ["bash", "-lc", PREFLIGHT_SCRIPT, "cassandra-remote-ci", self.remote_dir, *(commands or [])]
        output = self.ssh(script, capture=True).stdout
        if self.runner.dry:
            relative = self.remote_dir.removeprefix("~").removeprefix("/")
            self.base = self.remote_dir if self.remote_dir.startswith("/") else f"REMOTE_HOME/{relative}".rstrip("/")
            self.uv = "REMOTE_UV"
            return
        found = dict(line.split("=", 1) for line in output.splitlines() if "=" in line)
        self.base = found.get("base", "")
        self.uv = found.get("uv", "")
        self.active = found.get("active", "")
        self.docker_memory = int(found.get("docker memory") or 0)
        self.docker_os = found.get("docker os", "")
        problems = [] if self.uv else ["uv is not installed"]
        if commands is not None:
            problems += [f"{name} is not installed" for name in commands if found.get(f"command {name}") != "ok"]
            if found.get("docker access") != "ok":
                problems.append("docker info fails without sudo")
            if found.get("systemd user") != "ok":
                problems.append("systemctl --user doesn't work")
            elif found.get("docker in units") != "ok":
                problems.append("docker info fails in a systemd user unit; restart the user manager after usermod")
            if found.get("linger") != "yes":
                problems.append("lingering is off for the user")
        if problems:
            raise UsageError(
                f"{self.host} isn't ready:\n  " + "\n  ".join(problems) + f"\nsee 'Box setup' in {PROJECT_DIR / 'README.md'}"
            )

    def python_command(self, args: list[str]) -> list[str]:
        return [self.uv, "run", "--quiet", "--project", self.ci_dir, "python", "-m", BOX_MODULE, "--dir", self.base, *args]

    def python(self, args: list[str], **options) -> Result:
        return self.ssh(self.python_command(args), **options)

    def resolve(self, run_id: str | None) -> str:
        """Return run_id, else the run named in the remote lock, else the newest run."""
        if run_id:
            return run_id
        if self.runner.dry:
            return RUN_PLACEHOLDER
        return self.python(["resolve"], capture=True).stdout.strip()

    def progress_options(self) -> list[str]:
        if self.progress is None:
            self.progress = rsync_progress_options(self.runner)
        return self.progress

    def rsync_to(self, sources: list[str], path: str, backup_dir: str, excludes: Sequence[str] = ()) -> None:
        what = sources[0] if len(sources) == 1 else "the cassandra-remote-ci package"
        print(f"sync {what} to {self.host}:{path}", flush=True)
        self.runner.run(rsync_command(sources, f"{self.host}:{path}", backup_dir, excludes, self.progress_options()))

    def start_unit(self, run_id: str, args: list[str]) -> None:
        self.ssh(
            [
                "systemd-run",
                "--user",
                "--unit",
                unit_name(run_id),
                "--collect",
                "-p",
                f"WorkingDirectory={self.base}",
                "-p",
                f"TimeoutStopSec={STOP_TIMEOUT}",
                "-p",
                "KillMode=mixed",
                *self.python_command(["orchestrate", run_id, *args]),
            ]
        )
