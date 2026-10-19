# SPDX-License-Identifier: Apache-2.0

-include .env

HOST ?=
SRC  ?= .
JDK  ?=
DTEST ?=
MIN_CLASSES_PER_PART ?=
ARGS ?=
CLI  = uv run cassandra-remote-ci
RUN_OPTIONS = -H $(HOST) -C $(SRC) $(if $(JDK),-j $(JDK)) $(if $(MIN_CLASSES_PER_PART),--min-classes-per-part $(MIN_CLASSES_PER_PART))
RUN  = $(CLI) run $(RUN_OPTIONS)
PLUGIN_ARGS = $(if $(DTEST),-- -d $(DTEST))

.PHONY: help sync test run start dry-run status logs failures fetch abort retry cleanup install host

help: ## list targets
	@grep -hE '^[a-z-]+:.*## ' $(MAKEFILE_LIST) | sed -E 's/:.*## /\t/'

sync: ## uv sync
	uv sync

test: ## run the unit tests
	uv run python -m unittest discover -s tests/cassandra_remote_ci_tests -t tests

run: host ## run the tests on HOST and fetch the results
	$(RUN) $(ARGS) $(PLUGIN_ARGS)

start: host ## start a run on HOST and return
	$(CLI) start $(RUN_OPTIONS) $(ARGS) $(PLUGIN_ARGS)

dry-run: host ## print what run would do
	$(RUN) --dry-run $(ARGS) $(PLUGIN_ARGS)

status: host ## print the progress of the current run, ARGS=--watch repeats it
	$(CLI) status -H $(HOST) $(ARGS)

logs: host ## follow the run events, ARGS=--all adds the build, ARGS=<job>/part-<n> one part
	$(CLI) logs -H $(HOST) $(ARGS)

failures: host ## list failing tests
	$(CLI) failures -H $(HOST)

fetch: host ## copy the results to results/
	$(CLI) fetch -H $(HOST)

abort: host ## stop the current run
	$(CLI) abort -H $(HOST)

retry: host ## re-run crashed jobs, ARGS=--failed adds failed jobs
	$(CLI) retry -H $(HOST) $(ARGS)

cleanup: host ## move old runs to /tmp, e.g. ARGS="--older-than 7d --slots"
	$(CLI) cleanup -H $(HOST) $(ARGS)

install: ## put cassandra-remote-ci on PATH
	uv tool install --editable .

host:
	@test -n "$(HOST)" || { echo "HOST is required: set it in .env (see .env.example) or run make $(MAKECMDGOALS) HOST=ubuntu"; exit 1; }
