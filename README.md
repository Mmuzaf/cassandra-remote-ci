# cassandra-remote-ci

Runs a project's test suite on a remote Linux box in docker and copies the results back.

## Example

```bash
maxim.muzafarov@Maxims-MacBook-Pro cassandra-remote-ci % make status
uv run cassandra-remote-ci status -H ubuntu
run      20261019-2006-cassandra-5182b2b-jdk17
phase    running, unit active
elapsed  1h31m, eta ~8h33m
jobs     35 of 196 done, 8 running

BOX         NOW  AVERAGE  PEAK  TOTAL
========  =====  =======  ====  =====
CPU         82%      80%     -      -
memory    17.2G        -     -  90.0G
reserved  40.0G        -     -  82.0G

SLOT  JOB                PART  CLASSES  ELAPSED  LEFT  CONTAINER     STATE              CPU              MEM
====  =================  ====  =======  =======  ====  ============  =============  =======  ===============
   1  test-latest_17_20     2       43       6m   ~5m  3f83245a168a  Up 6 minutes   209.80%   1.62GiB / 5GiB
   2  test-latest_15_20     2       43       8m   ~3m  0bb9aa4e5ae5  Up 8 minutes   225.53%  939.3MiB / 5GiB
   3  test-latest_17_20     1       44       6m   ~5m  7888e3ae6673  Up 6 minutes     4.74%  1.502GiB / 5GiB
   4  test-latest_15_20     1       44       8m   ~2m  ce2053c6e3ea  Up 8 minutes   256.53%  1.602GiB / 5GiB
   5  test-latest_13_20     1       87      10m   ~9m  3124df50ebad  Up 10 minutes  197.56%  1.519GiB / 5GiB
   6  test-latest_16_20     1       44       7m   ~3m  942502feb8bd  Up 7 minutes    82.59%   2.11GiB / 5GiB
   7  test-latest_16_20     2       43       6m   ~3m  5d1b39ee8983  Up 6 minutes   230.47%  1.371GiB / 5GiB
   8  test-latest_14_20     1       87       9m  ~12m  645ff55c8271  Up 9 minutes   112.79%   1.72GiB / 5GiB

GROUP                 JOBS      CLASSES           TESTS  FAIL  ERR  SKIP
==================  ======  ===========  ==============  ====  ===  ====
fqltool-test           1/1            3          27/~27     0    0     0
sstableloader-test     1/1            1          13/~13     0    0     0
test                 20/20  1,738/1,738  17,188/~17,188     0    1    91
test-latest          12/20  1,313/1,738  13,315/~17,167     0    0   203
stress-test            1/1            8          22/~22     0    0     0
test-burn              0/5         0/11           0/~59     0    0     0
jvm-dtest             0/16        0/441        0/~2,907     0    0     0
dtest                 0/64            0        0/~1,107     0    0     0
dtest-latest          0/64            0        0/~1,107     0    0     0
microbench-test        0/4            0               0     0    0     0
------------------  ------  -----------  --------------  ----  ---  ----
total               35/196  3,051/3,928          30,565     0    1   294

RECENT_FAILURE                                                             STATUS  GROUP  JOB
=========================================================================  ======  =====  =========
o.a.c.audit.AuditLoggerCleanupTest.testCleanupOfAuditLogDir-_jdk17_x86_64  error   test   test_6_20

Activity
  slot1  class o.a.c.tools.nodetool.NodetoolHelpCommandsOutputTest
  slot2  class o.a.c.utils.memory.TrieMemtableShardLockDeadlockTest
  slot3  class o.a.c.db.virtual.AccordDebugKeyspaceTest
  slot4  class o.a.c.db.guardrails.GuardrailZstdCompressionLevelTest
         [junit-timeout]
  slot5  class o.a.c.index.sai.cql.types.collections.maps.MapKeysIntTest
  slot6  class o.a.c.db.compaction.unified.ShardedCompactionWriterTest
  slot7  class o.a.c.utils.concurrent.RefCountedTest
  slot8  class o.a.c.index.CustomIndexTest

Log
  21:31:16 start test-latest_17_20 part-2 with 43 classes in slot1
```

## Requirements

- Mac: [uv](https://docs.astral.sh/uv/), `ssh`, `rsync`, and an ssh host alias for the box (examples use `ubuntu`).
- Box: Ubuntu with the setup below. uv installs Python 3.12 on first use.

## Box setup (run once on the box, by hand)

The tool only checks these, and never changes the box.

```bash
sudo apt-get update
sudo apt-get install -y docker.io rsync xz-utils git bc
sudo usermod -aG docker $USER            # log out and back in afterwards
sudo loginctl enable-linger $USER        # runs survive logout
curl -LsSf https://astral.sh/uv/install.sh | sh
```

If Docker CE from download.docker.com is installed, `containerd.io` conflicts with `docker.io`. Drop `docker.io` from the line and keep Docker CE.

Check:

```bash
docker run --rm hello-world
systemctl --user status >/dev/null && echo ok
loginctl show-user $USER -p Linger
~/.local/bin/uv --version
systemd-run --user --wait --quiet docker info >/dev/null && echo ok
```

If the last check fails, the user manager started before `usermod`. Restart it with `sudo systemctl restart user@$(id -u)`.

## Run

```bash
# Cassandra pre-commit tests on JDK 17, up to 8 containers at once, wait and fetch the results
uv run cassandra-remote-ci run -H ubuntu -C ~/IdeaProjects/cassandra -j 17

# start in the background, check on it later
uv run cassandra-remote-ci start -H ubuntu -C ~/IdeaProjects/cassandra -j 17
uv run cassandra-remote-ci status -H ubuntu --watch
uv run cassandra-remote-ci failures -H ubuntu
uv run cassandra-remote-ci logs -H ubuntu                    # run events; --all adds the prepare build
uv run cassandra-remote-ci logs -H ubuntu jvm-dtest_3_16/part-1
uv run cassandra-remote-ci fetch -H ubuntu

# only some jobs
uv run cassandra-remote-ci run -H ubuntu -C ~/IdeaProjects/cassandra -j 17 --only '^jvm-dtest_'

# print the commands and the job list, touch nothing
uv run cassandra-remote-ci run -H ubuntu -C ~/IdeaProjects/cassandra -j 17 --dry-run

# stop, or re-run what crashed (add --failed for failed jobs too)
uv run cassandra-remote-ci abort -H ubuntu
uv run cassandra-remote-ci retry -H ubuntu

# move old runs and slot copies to /tmp/cassandra-remote-ci-trash/ (nothing is deleted);
# job durations stay in ~/cassandra-remote-ci/stats/jobs.jsonl on the box for the ETA
uv run cassandra-remote-ci cleanup -H ubuntu --older-than 7d --slots

# optional: put cassandra-remote-ci on PATH
uv tool install --editable .
```

The same through make. `HOST`, `SRC`, `JDK`, `DTEST` and `MIN_CLASSES_PER_PART` can live in `.env` (copy
`.env.example`); variables on the command line override it. `DTEST` is the local cassandra-dtest checkout, passed as
`-- -d <dir>`:

```bash
cp .env.example .env
make run
make dry-run JDK=11
make status
```

## Results

`results/<RUN_ID>/`: `summary.md`, `failures.txt`, `crashed-jobs.txt`, `report/ci_summary.html`, logs of failed jobs.

`summary.md` gives the box's average and peak CPU and memory use while jobs ran, sampled every 30 seconds into
`usage.json` and carried on across retries. Status shows the averages so far.

A crashed job exited non-zero without a failing test. `summary.md`, `crashed-jobs.txt`, `make failures` and the "Crashed jobs"
table at the end of `ci_summary.html` give its cause: the first error the console log reports after the last Cassandra log
line, before ant's `BUILD FAILED`.

## Options

`uv run cassandra-remote-ci --help`

## Plugins

`cassandra` (complete), `opensearch` (stubs). To add one, subclass `Plugin` in
[src/cassandra_remote_ci/shared/plugin.py](src/cassandra_remote_ci/shared/plugin.py).

## Layout

Code is split by where it runs, see [src/cassandra_remote_ci/README.md](src/cassandra_remote_ci/README.md).

## Tests

```bash
make test
```

Tests mirror the package under `tests/cassandra_remote_ci_tests/`.
