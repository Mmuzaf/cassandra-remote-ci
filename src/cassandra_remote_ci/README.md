# cassandra_remote_ci

Each subdirectory is either a component, which runs on one side, or a shared module.
Components don't import each other; code they both need goes into `shared`.

## Shared modules

- `shared` - used by both sides and by plugins; imports nothing else from the package.
  - `plugin.py` - the `Plugin` base class, `Job`, `RunContext`, and the plugin loader
  - `state.py` - the files of a run on the box, the lock, and JSON helpers
  - `junit.py` - JUnit XML parsing and counts
  - `units.py` - durations and sizes as people read them

## Components

- `local` - the command line on the Mac
  - `cli.py` - arguments and commands
  - `box.py` - `Remote`: preflight checks, ssh and rsync to the box, the systemd unit of a run
  - `ssh.py` - the command runner, and ssh and rsync command lines
  - `render.py` - the status text, from the JSON the box returns
- `box` - runs on the box, from a copy `local` syncs there at every start
  - `commands.py` - the commands `local` runs over ssh with `python -m cassandra_remote_ci.box.commands`
  - `orchestrate.py` - prepare, slots, the job queue within the memory budget, aggregation
  - `usage.py` - CPU and memory sampling, and the peak memory of each job's containers
  - `progress.py` - the status snapshot of a run
  - `stats.py` - durations of earlier runs and the ETA
  - `summary.py` - failure lists, crash causes, `summary.md`
- `plugins` - one subpackage per project, exposing `PLUGIN`; plugins import only from `shared`

Tests mirror this tree under `tests/cassandra_remote_ci_tests/`.
