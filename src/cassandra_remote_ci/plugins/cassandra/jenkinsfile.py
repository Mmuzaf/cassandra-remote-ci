# SPDX-License-Identifier: Apache-2.0
"""Pipeline profiles and test splits of a Cassandra checkout, read from .jenkins/Jenkinsfile and build.xml."""

from __future__ import annotations

import re
from pathlib import Path

from cassandra_remote_ci.shared.plugin import Job, PluginError

BUILD_STEPS = {"jar", "artifacts", "lint", "debian", "redhat"}
DEFAULT_JDK_ONLY = {"cqlsh-test", "simulator-dtest"}


def build_property(build_xml: Path, name: str) -> str:
    match = re.search(rf'property\s+name="{re.escape(name)}"\s+value="([^"]*)"', build_xml.read_text(errors="replace"))
    if not match:
        raise PluginError(f"{build_xml} has no {name} property")
    return match.group(1)


def _block(text: str, start: str, end: str) -> str:
    begin = text.find(start)
    if begin < 0:
        raise PluginError(f"the Jenkinsfile has no {start!r}")
    finish = text.find(end, begin)
    return text[begin : finish if finish > 0 else len(text)]


def read_profiles(text: str) -> dict[str, list[str]]:
    block = _block(text, "def pipelineProfiles()", "\n}")
    entries = re.findall(r"^\s*'([^']+)'\s*:\s*\[([^\]]*)\]", block, re.MULTILINE)
    return {name: re.findall(r"'([^']+)'", steps) for name, steps in entries}


def read_splits(text: str) -> dict[str, int]:
    block = _block(text, "def testSteps = [", "testSteps.each")
    return {name: int(count) for name, count in re.findall(r"^\s*'([\w-]+)'\s*:\s*\[\s*splits:\s*(\d+)", block, re.MULTILINE)}


def read_sizes(text: str) -> dict[str, str]:
    """Return the agent size of each test step, medium where the step names none, as testSteps.each sets it."""
    block = _block(text, "def testSteps = [", "testSteps.each")
    steps = re.findall(r"^\s*'([\w-]+)'\s*:\s*\[([^\]]*)\]", block, re.MULTILINE)
    return {name: next(iter(re.findall(r"size:\s*'(\w+)'", options)), "medium") for name, options in steps}


def select_jobs(text: str, profile: str, jdk: str, default_jdk: str, with_upgrade: bool) -> list[Job]:
    """Return one job per split of each test step in the profile, without upgrade tests unless asked."""
    profiles = read_profiles(text)
    if profile not in profiles:
        raise PluginError(f"unknown profile {profile!r}; the Jenkinsfile has {', '.join(profiles)}")
    splits = read_splits(text)
    jobs = []
    for step in profiles[profile]:
        if step in BUILD_STEPS or step not in splits:
            continue
        if "upgrade" in step and not with_upgrade:
            continue
        if jdk != default_jdk and (step in DEFAULT_JDK_ONLY or "dtest-upgrade" in step):
            continue
        count = splits[step]
        jobs += [Job(f"{step}_{index}_{count}", step, [step, f"{index}/{count}"]) for index in range(1, count + 1)]
    return jobs
