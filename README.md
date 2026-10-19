# cassandra-remote-ci

Runs a project's test suite on a remote Linux box in docker and copies the results back.

## Example

```bash
maxim.muzafarov@Maxims-MacBook-Pro cassandra-remote-ci % make status
uv run cassandra-remote-ci status -H ubuntu
20261017-2258-cassandra-5182b2b-jdk17: running, elapsed 3h04m, eta ~9h30m, 48 of 196 jobs done, 5 running, unit active
----------------------------------------------------------------------------------------------------------------------

SLOT  JOB             PART  CLASSES  ELAPSED  LEFT  CONTAINER     STATE              CPU               MEM
====  ==============  ====  =======  =======  ====  ============  =============  =======  ================
   1  jvm-dtest_3_16     1       28      22m  ~23m  b44a3208d875  Up 22 minutes  325.53%  2.528GiB / 16GiB
   2  jvm-dtest_4_16     1       28      18m  ~28m  3f96209ec449  Up 18 minutes  210.27%  2.286GiB / 16GiB
   3  jvm-dtest_1_16     1       28      38m   ~4m  6326ee292dde  Up 38 minutes  271.32%  2.333GiB / 16GiB
   4  jvm-dtest_5_16     1       28       5m  ~40m  5794d0e6a2e7  Up 5 minutes   180.22%  1.686GiB / 16GiB
   5  jvm-dtest_2_16     1       28      31m  ~17m  bd0d64751644  Up 31 minutes  231.78%  1.814GiB / 16GiB

GROUP                 JOBS      CLASSES           TESTS  FAIL  ERR  SKIP
==================  ======  ===========  ==============  ====  ===  ====
fqltool-test           1/1            3          27/~27     0    0     0
sstableloader-test     1/1            1          13/~13     0    0     0
test                 20/20  1,842/1,842  17,188/~17,188     0    0    91
test-latest          20/20  1,841/1,841  17,167/~17,167     0    0   262
stress-test            1/1            8          22/~22     0    0     0
test-burn              5/5        11/11          59/~59     0    0     1
jvm-dtest             0/16       68/441        948/~702     0    1     7
dtest                 0/64            0           0/~18     0    0     0
dtest-latest          0/64            0           0/~18     0    0     0
microbench-test        0/4            0               0     0    0     0
------------------  ------  -----------  --------------  ----  ---  ----
total               48/196  3,762/3,928          35,424     0    1   361

RECENT_FAILURE                                                                 STATUS  GROUP      JOB
=============================================================================  ======  =========  ==============
org.apache.cassandra.distributed.test.accord.NewSchemaTest.test-_jdk17_x86_64  error   jvm-dtest  jvm-dtest_1_16

Activity
  slot1  class org.apache.cassandra.distributed.test.cql3.MultiNodeTableWalkWithReadRepairTest
  slot2  class org.apache.cassandra.distributed.test.accord.AccordRegainRangesTest
  slot3  class org.apache.cassandra.distributed.test.log.mso.AllowIPChangeWithMoveTest
  slot4  class org.apache.cassandra.distributed.test.IPMembershipTest
  slot5  class org.apache.cassandra.distributed.test.cql3.MixedReadsAccordInteropMultiNodeTokenConflictTest

Log
  01:56:55 start jvm-dtest_5_16 part-1 with 28 classes in slot4
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
