# SPDX-License-Identifier: Apache-2.0
"""Command line on the Mac: sync a checkout to the box, run its tests there, and fetch the results."""

from __future__ import annotations

import argparse
import json
import re
import shutil
import sys
import time
from collections import Counter
from pathlib import Path

from cassandra_remote_ci.local import render
from cassandra_remote_ci.local.box import PROJECT_DIR, STOP_TIMEOUT, Remote, UsageError
from cassandra_remote_ci.local.ssh import CommandError, CommandRunner, DryRunRunner, rsync_command
from cassandra_remote_ci.shared.plugin import Job, Plugin, PluginError, RunContext, detect_plugin, filter_jobs, load_plugin
from cassandra_remote_ci.shared.state import DEFAULT_MIN_CLASSES_PER_PART, FINISHED_EXIT, unit_name
from cassandra_remote_ci.shared.units import gigabytes

PACKAGE_FILES = ("pyproject.toml", "uv.lock", ".python-version", "README.md", "src")
EVENT_LINE = "^[0-9]{2}:[0-9]{2}:[0-9]{2} "
COMMANDS = ("run", "start", "status", "failures", "logs", "abort", "retry", "fetch", "cleanup")
BOX_COMMANDS = ["docker", "rsync", "xz", "git"]
AGE_UNITS = {"s": 1, "m": 60, "h": 3600, "d": 86400, "w": 604800}
# Memory docker has that the jobs don't reserve: the box, the orchestrator and the page cache.
MEMORY_RESERVE = 8 * 2**30


def stamp() -> str:
    return time.strftime("%Y%m%d-%H%M%S")


def parse_age(text: str) -> int:
    """Return the seconds in an age such as 90m, 12h or 7d."""
    match = re.fullmatch(r"(\d+)([smhdw]?)", text)
    if not match:
        raise UsageError(f"can't read age {text!r}; use a number with s, m, h, d or w, e.g. 7d")
    return int(match.group(1)) * AGE_UNITS[match.group(2) or "s"]


def memory_budget(remote: Remote, plugin: Plugin, ctx: RunContext, jobs: list[Job]) -> int | None:
    """Return the memory the running jobs may reserve together, docker's less MEMORY_RESERVE, None if unknown.

    Fail if the largest job doesn't fit.
    """
    if not remote.docker_memory:
        return None
    budget = remote.docker_memory - MEMORY_RESERVE
    largest = max(jobs, key=lambda job: plugin.job_memory(ctx, job))
    needed = plugin.job_memory(ctx, largest)
    if needed > budget:
        raise UsageError(
            f"docker on {remote.host} ({remote.docker_os}) has {gigabytes(remote.docker_memory)} of memory, "
            f"{largest.id} needs {gigabytes(needed)} and the box keeps {gigabytes(MEMORY_RESERVE)}; lower -m, "
            "or on Docker Desktop switch the box to the native engine with 'docker context use default'"
        )
    return budget


def git_head(src: Path) -> str:
    """Return the short sha of HEAD in src by reading .git, or 'nogit'."""
    git = src / ".git"
    if git.is_file():
        git = (src / git.read_text().split("gitdir:", 1)[-1].strip()).resolve()
    try:
        head = (git / "HEAD").read_text().strip()
    except OSError:
        return "nogit"
    if not head.startswith("ref: "):
        return head[:7]
    ref = head.removeprefix("ref: ")
    common = (git / (git / "commondir").read_text().strip()).resolve() if (git / "commondir").exists() else git
    for directory in (git, common):
        if (directory / ref).exists():
            return (directory / ref).read_text().strip()[:7]
    packed = common / "packed-refs"
    if packed.exists():
        for line in packed.read_text().splitlines():
            if line.endswith(" " + ref):
                return line[:7]
    return "nogit"


def git_ignored(runner: CommandRunner, path: Path, prefix: str = "") -> list[str]:
    """Return the untracked files git ignores in path and its submodules as anchored rsync excludes.

    None if path isn't a git checkout, e.g. an uninitialized submodule.
    """
    if not (path / ".git").exists():
        return []
    git = ["git", "-C", str(path)]
    ignored = runner.run([*git, "ls-files", "-z", "--others", "--ignored", "--exclude-standard", "--directory"], capture=True)
    excludes = [f"/{prefix}{name}" for name in ignored.stdout.split("\0") if name]
    modules = runner.run([*git, "config", "-z", "-f", ".gitmodules", "--get-regexp", r"\.path$"], capture=True, check=False)
    for entry in filter(None, modules.stdout.split("\0")):
        module = entry.split("\n", 1)[1]
        excludes += git_ignored(runner, path / module, f"{prefix}{module}/")
    return excludes


def check_local_tools(runner: CommandRunner) -> None:
    missing = [name for name in ("ssh", "rsync") if not shutil.which(name)]
    if missing and not runner.dry:
        raise UsageError(f"install {' and '.join(missing)} on this machine")


def plan_run(args: argparse.Namespace) -> tuple[Plugin, RunContext, list[Job]]:
    """Pick the plugin, build the RunContext and list the jobs, reading only local files."""
    src = args.src.expanduser().resolve()
    if not src.is_dir():
        raise UsageError(f"{src} is not a directory")
    plugin = load_plugin(args.plugin) if args.plugin else detect_plugin(src)
    jdk = str(args.jdk or plugin.default_jdk(src))
    run_id = f"{time.strftime('%Y%m%d-%H%M')}-{plugin.name}-{git_head(src)}-jdk{jdk}"
    ctx = RunContext(
        run_id=run_id,
        run_dir=args.out_dir.expanduser() / run_id,
        src=src,
        jdk=jdk,
        memory=args.memory or plugin.default_memory,
        parallel=args.parallel or plugin.default_parallel,
        profile=plugin.default_profile if args.profile is None else args.profile,
        plugin_args=args.plugin_args,
        trash_dir=Path(args.trash_dir),
    )
    jobs = filter_jobs(plugin.jobs(ctx), args.only)
    if not jobs:
        raise UsageError(f"no jobs match --only {args.only!r}")
    return plugin, ctx, jobs


def print_plan(plugin: Plugin, ctx: RunContext, jobs: list[Job]) -> None:
    print(f"run      {ctx.run_id}")
    print(f"plugin   {plugin.name}, profile {ctx.profile or '-'}, JDK {ctx.jdk}")
    print(f"slots    {ctx.parallel}, each job reserving the memory of its container")
    memory = {job.group: plugin.job_memory(ctx, job) for job in jobs}
    for group, count in Counter(job.group for job in jobs).items():
        print(f"  {group:<34}{count:>5}  {gigabytes(memory[group]):>6}")
    print(f"jobs     {len(jobs)} total")


def start(args: argparse.Namespace, remote: Remote) -> str:
    """Sync the checkout and the package to the box, start the systemd unit, and return the RUN_ID."""
    if args.min_classes_per_part < 1:
        raise UsageError("--min-classes-per-part must be at least 1")
    plugin, ctx, jobs = plan_run(args)
    print_plan(plugin, ctx, jobs)
    extra = plugin.extra_sources(ctx.src, ctx.plugin_args)
    check_local_tools(remote.runner)
    remote.preflight([*BOX_COMMANDS, *plugin.remote_commands])
    budget = memory_budget(remote, plugin, ctx, jobs)
    if budget:
        print(f"memory   {gigabytes(budget)} for the jobs of docker's {gigabytes(remote.docker_memory)}")
    if remote.active:
        raise UsageError(f"remote lock held by run {remote.active}; use 'abort' or wait")

    trash = f"{args.trash_dir}/{stamp()}"
    remote.ssh(["mkdir", "-p", f"{remote.base}/runs", remote.ci_dir])
    remote.rsync_to(
        [str(PROJECT_DIR / name) for name in PACKAGE_FILES], f"{remote.ci_dir}/", f"{trash}/ci", ["/.venv/", "__pycache__/"]
    )
    src_excludes = [*plugin.sync_excludes, *git_ignored(remote.runner, ctx.src)]
    remote.rsync_to([f"{ctx.src}/"], f"{remote.base}/src/", f"{trash}/src", src_excludes)
    for name, path in extra.items():
        remote.rsync_to([f"{path}/"], f"{remote.base}/{name}/", f"{trash}/{name}", git_ignored(remote.runner, path))

    dirty = None
    if not remote.runner.dry:
        status = remote.runner.run(["git", "-C", str(ctx.src), "status", "--porcelain"], capture=True, check=False)
        dirty = bool(status.stdout.strip())
    info = {
        "run_id": ctx.run_id,
        "plugin": plugin.name,
        "jdk": ctx.jdk,
        "memory": ctx.memory,
        "memory_budget": budget,
        "parallel": ctx.parallel,
        "profile": ctx.profile,
        "only": args.only,
        "min_classes_per_part": args.min_classes_per_part,
        "plugin_args": ctx.plugin_args,
        "trash_dir": trash,
        "git_sha": git_head(ctx.src),
        "dirty": dirty,
        "local_src": str(ctx.src),
        "host": remote.host,
        "created": time.time(),
    }
    remote.python(["prepare-run", ctx.run_id], input=json.dumps(info), capture=True)
    remote.start_unit(ctx.run_id, [])
    print(f"started run {ctx.run_id} on {remote.host}")
    return ctx.run_id


def show_status(remote: Remote, run_id: str, as_json: bool, clear: bool = False) -> int:
    """Print the status of a run and return the remote exit code, FINISHED_EXIT once the run has stopped.

    The box returns JSON and the text is rendered here, so its layout follows this checkout even while the box
    runs an older copy of the package.
    """
    result = remote.python(["status", run_id, "--json"], capture=True, check=False)
    if result.returncode not in (0, FINISHED_EXIT):
        raise UsageError(f"status of run {run_id} failed with exit code {result.returncode}")
    if clear and sys.stdout.isatty():
        print("\033[2J\033[H", end="")
    if as_json or remote.runner.dry:
        print(result.stdout, end="")
    else:
        data = json.loads(result.stdout)
        print(render.render(data, data.get("unit")))
    return result.returncode


def watch(remote: Remote, run_id: str, interval: int, as_json: bool = False) -> None:
    while show_status(remote, run_id, as_json, clear=not as_json) != FINISHED_EXIT and not remote.runner.dry:
        time.sleep(interval)


def fetch_run(args: argparse.Namespace, remote: Remote, run_id: str) -> None:
    listing = remote.python(["fetch-list", run_id, *(["--all-logs"] if args.all_logs else [])], capture=True).stdout
    destination = args.out_dir.expanduser() / run_id
    if not remote.runner.dry:
        destination.mkdir(parents=True, exist_ok=True)
    remote.runner.run(
        rsync_command(
            [f"{remote.host}:{remote.run_dir(run_id)}/"],
            f"{destination}/",
            options=["-r", "--files-from=-", *remote.progress_options()],
        ),
        input=listing,
    )
    print(f"results in {destination}")
    summary = destination / "summary.md"
    if summary.exists():
        print(summary.read_text(), end="")


def cmd_run(args: argparse.Namespace, remote: Remote) -> int:
    run_id = start(args, remote)
    watch(remote, run_id, args.watch)
    fetch_run(args, remote, run_id)
    return 0


def cmd_start(args: argparse.Namespace, remote: Remote) -> int:
    start(args, remote)
    return 0


def cmd_status(args: argparse.Namespace, remote: Remote) -> int:
    remote.preflight()
    run_id = remote.resolve(args.run_id)
    if args.watch:
        watch(remote, run_id, args.watch, args.json)
    else:
        show_status(remote, run_id, args.json)
    return 0


def cmd_failures(args: argparse.Namespace, remote: Remote) -> int:
    remote.preflight()
    remote.python(["failures", remote.resolve(args.run_id), *(["--json"] if args.json else [])])
    return 0


def cmd_logs(args: argparse.Namespace, remote: Remote) -> int:
    remote.preflight()
    run_id = remote.resolve(args.run_id)
    if args.job_id:
        log = f"{remote.run_dir(run_id)}/jobs/{args.job_id}/console.log"
        remote.ssh(["tail", "-n", "200", "-F", log], check=False, tty=True)
    else:
        events = [] if args.all else ["-o", "cat", "--grep", EVENT_LINE]
        remote.ssh(["journalctl", "--user", "-u", unit_name(run_id), "-n", "200", "-f", *events], check=False, tty=True)
    return 0


def cmd_abort(args: argparse.Namespace, remote: Remote) -> int:
    remote.preflight()
    run_id = remote.resolve(args.run_id)
    print(f"stopping {unit_name(run_id)}; systemd kills it after {STOP_TIMEOUT} if it doesn't exit", flush=True)
    remote.ssh(["systemctl", "--user", "stop", unit_name(run_id)], check=False)
    remote.python(["stop-containers", run_id])
    return 0


def cmd_retry(args: argparse.Namespace, remote: Remote) -> int:
    remote.preflight(BOX_COMMANDS)
    run_id = remote.resolve(args.run_id)
    if remote.active:
        raise UsageError(f"remote lock held by run {remote.active}; use 'abort' or wait")
    remote.start_unit(run_id, ["--retry", *(["--failed"] if args.failed else [])])
    print(f"retrying run {run_id} on {remote.host}")
    return 0


def cmd_fetch(args: argparse.Namespace, remote: Remote) -> int:
    remote.preflight()
    fetch_run(args, remote, remote.resolve(args.run_id))
    return 0


def cmd_cleanup(args: argparse.Namespace, remote: Remote) -> int:
    """Move runs and slot copies on the box into the trash directory and print how to delete them."""
    if not (args.runs or args.older_than or args.slots):
        raise UsageError("pass --runs, --older-than or --slots")
    remote.preflight()
    trash = f"{args.trash_dir}/{stamp()}"
    if remote.ssh(["findmnt", "-no", "FSTYPE", "/tmp"], capture=True, check=False).stdout.strip() == "tmpfs":
        print(f"warning: /tmp on {remote.host} is a tmpfs; moved files take up RAM until you delete them")
        if args.slots and not args.force:
            raise UsageError("refusing to move slots into a tmpfs /tmp; pass --force, or --trash-dir on a disk")

    runs = list(args.runs or [])
    if args.older_than:
        if remote.runner.dry:
            runs.append(f"RUNS_OLDER_THAN_{args.older_than}")
        else:
            listed = remote.python(["list-runs", "--older-than", str(parse_age(args.older_than))], capture=True)
            runs += [run_id for run_id in listed.stdout.split() if run_id not in runs]
    if remote.active and remote.active in runs:
        raise UsageError(f"run {remote.active} is active; abort it first")
    slots = []
    if args.slots:
        if remote.active:
            raise UsageError(f"run {remote.active} is using the slots; abort it or wait")
        if remote.runner.dry:
            slots = [f"{remote.base}/SLOT_DIRS"]
        else:
            found = remote.ssh(["find", remote.base, "-maxdepth", "1", "-type", "d", "-name", "slot*"], capture=True)
            slots = sorted(found.stdout.split())
    if not runs and not slots:
        print("nothing to move")
        return 0

    remote.ssh(["mkdir", "-p", f"{trash}/runs"])
    if runs:
        remote.ssh(["mv", *[remote.run_dir(run_id) for run_id in runs], f"{trash}/runs/"])
    if slots:
        remote.ssh(["mv", *slots, f"{trash}/"])
    verb = "would move" if remote.runner.dry else "moved"
    print(f"{verb} {len(runs)} runs and {len(slots)} slots to {remote.host}:{trash}")
    remote.ssh(["du", "-sh", trash], check=False)
    print(f"to delete them: ssh {remote.host} 'rm -rf {trash}'")
    return 0


def cleanup_local(args: argparse.Namespace, runner: CommandRunner) -> int:
    """Move fetched runs under the results directory into the trash directory."""
    root = args.out_dir.expanduser()
    targets = [root / run_id for run_id in args.runs or []]
    if args.older_than and root.is_dir():
        cutoff = time.time() - parse_age(args.older_than)
        targets += [path for path in sorted(root.iterdir()) if path.is_dir() and path.stat().st_mtime < cutoff]
    targets = list(dict.fromkeys(targets))
    if not targets:
        print("nothing to move")
        return 0
    trash = f"{args.trash_dir}/{stamp()}"
    runner.run(["mkdir", "-p", f"{trash}/results"])
    runner.run(["mv", *map(str, targets), f"{trash}/results/"])
    print(f"{'would move' if runner.dry else 'moved'} {len(targets)} runs to {trash}")
    runner.run(["du", "-sh", trash], check=False)
    print(f"to delete them: rm -rf {trash}")
    return 0


HANDLERS = {
    "run": cmd_run,
    "start": cmd_start,
    "status": cmd_status,
    "failures": cmd_failures,
    "logs": cmd_logs,
    "abort": cmd_abort,
    "retry": cmd_retry,
    "fetch": cmd_fetch,
    "cleanup": cmd_cleanup,
}


def build_parser() -> argparse.ArgumentParser:
    connection = argparse.ArgumentParser(add_help=False)
    connection.add_argument("-H", "--host", help="ssh host of the box")
    connection.add_argument("-r", "--remote-dir", default="~/cassandra-remote-ci", help="directory on the box (%(default)s)")
    connection.add_argument("-o", "--out-dir", type=Path, default=PROJECT_DIR / "results", help="where fetch puts runs")
    connection.add_argument(
        "--trash-dir", default="/tmp/cassandra-remote-ci-trash", help="where removed files go, on the machine they were on"
    )
    connection.add_argument("--dry-run", action="store_true", help="print the commands instead of running them")

    selected = argparse.ArgumentParser(add_help=False)
    selected.add_argument("--run", dest="run_id", help="RUN_ID; defaults to the active run, else the newest")

    run_options = argparse.ArgumentParser(add_help=False)
    run_options.add_argument("-C", "--src", type=Path, default=Path("."), help="checkout to test (current directory)")
    run_options.add_argument("-P", "--plugin", help="plugin name; detected from the checkout if omitted")
    run_options.add_argument("-j", "--jdk", help="JDK version; the plugin's default if omitted")
    run_options.add_argument("-p", "--parallel", type=int, help="containers at once; the plugin's default if omitted")
    run_options.add_argument("-m", "--memory", help="memory of every container, e.g. 16g; the plugin's per-job memory if omitted")
    run_options.add_argument("--profile", help="test profile; the plugin's default if omitted")
    run_options.add_argument("--only", help="regexp matched against '<job id> <args>'")
    run_options.add_argument(
        "--min-classes-per-part",
        type=int,
        default=DEFAULT_MIN_CLASSES_PER_PART,
        metavar="N",
        help="fewest test classes a free slot takes from a job that runs in parts (%(default)s)",
    )
    run_options.add_argument("--watch", type=int, default=60, metavar="SECONDS", help="status interval (%(default)s)")
    run_options.add_argument("--all-logs", action="store_true", help="fetch the logs of every job, not only failed ones")

    parser = argparse.ArgumentParser(
        prog="cassandra-remote-ci",
        description="Run a project's tests on a remote box in docker and fetch the results.",
        epilog="Without a command, runs 'run'. Arguments after -- go to the plugin.",
    )
    commands = parser.add_subparsers(dest="command", metavar="command")
    commands.add_parser("run", parents=[connection, run_options], help="start, show progress until done, then fetch")
    commands.add_parser("start", parents=[connection, run_options], help="sync and start a run in the background")
    command = commands.add_parser("status", parents=[connection, selected], help="show the progress of a run")
    command.add_argument("--watch", nargs="?", const=60, type=int, metavar="SECONDS", help="repeat until the run stops")
    command.add_argument("--json", action="store_true")
    command = commands.add_parser("failures", parents=[connection, selected], help="list failing tests and crashed jobs")
    command.add_argument("--json", action="store_true")
    command = commands.add_parser("logs", parents=[connection, selected], help="follow the run's log, or one job's")
    command.add_argument("job_id", nargs="?", help="a job, or a part of one as <job>/part-<n>")
    command.add_argument("--all", action="store_true", help="the whole run log, with the prepare build, not only events")
    commands.add_parser("abort", parents=[connection, selected], help="stop a run and its containers")
    command = commands.add_parser("retry", parents=[connection, selected], help="re-run crashed and never-run jobs")
    command.add_argument("--failed", action="store_true", help="also re-run jobs with failing tests")
    command = commands.add_parser("fetch", parents=[connection, selected], help="copy a run's results here")
    command.add_argument("--all-logs", action="store_true", help="fetch the logs of every job, not only failed ones")
    command = commands.add_parser("cleanup", parents=[connection], help="move old runs and slots into the trash directory")
    command.add_argument("--runs", nargs="+", metavar="RUN_ID")
    command.add_argument("--older-than", metavar="AGE", help="runs older than AGE, e.g. 7d")
    command.add_argument("--slots", action="store_true", help="the slot copies on the box")
    command.add_argument("--local", action="store_true", help="act on the results directory here")
    command.add_argument("--force", action="store_true", help="move slots even into a tmpfs /tmp")
    return parser


def split_argv(argv: list[str]) -> tuple[list[str], list[str]]:
    """Split off plugin arguments after the first --, and move the command to the front, defaulting to run."""
    plugin_args: list[str] = []
    if "--" in argv:
        plugin_args = [arg for arg in argv[argv.index("--") + 1 :] if arg != "--"]
        argv = argv[: argv.index("--")]
    for index, token in enumerate(argv):
        if token in COMMANDS:
            return [token, *argv[:index], *argv[index + 1 :]], plugin_args
    if argv in ([], ["-h"], ["--help"]):
        return argv, plugin_args
    return ["run", *argv], plugin_args


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    argv, plugin_args = split_argv(list(sys.argv[1:] if argv is None else argv))
    if not argv:
        parser.print_help()
        return 2
    args = parser.parse_args(argv)
    args.plugin_args = plugin_args
    runner = DryRunRunner() if args.dry_run else CommandRunner()
    try:
        if args.command == "cleanup" and args.local:
            return cleanup_local(args, runner)
        if not args.host:
            raise UsageError("pass -H <host>")
        return HANDLERS[args.command](args, Remote(args.host, runner, args.remote_dir))
    except (UsageError, PluginError, NotImplementedError, CommandError, LookupError) as error:
        print(f"error: {error}", file=sys.stderr)
        return 1
    except KeyboardInterrupt:
        print("\ninterrupted; a started run keeps going on the box", file=sys.stderr)
        return 130
