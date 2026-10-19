# SPDX-License-Identifier: Apache-2.0
"""JUnit-format XML and events files to test results and counts."""

from __future__ import annotations

import xml.etree.ElementTree as ElementTree
from dataclasses import asdict, dataclass
from pathlib import Path

STATUSES = ("passed", "failure", "error", "skipped")


@dataclass(frozen=True)
class TestResult:
    """One test case. suite is the class ant ran, one per file; its cases may name other classes, e.g. nested ones."""

    classname: str
    name: str
    status: str
    message: str = ""
    suite: str = ""

    @property
    def test(self) -> str:
        return f"{self.classname}.{self.name}" if self.classname else self.name


@dataclass
class Counts:
    tests: int = 0
    failures: int = 0
    errors: int = 0
    skipped: int = 0
    classes: int = 0

    def add(self, other: Counts) -> None:
        self.tests += other.tests
        self.failures += other.failures
        self.errors += other.errors
        self.skipped += other.skipped
        self.classes += other.classes

    def to_dict(self) -> dict:
        return asdict(self)

    @staticmethod
    def from_dict(data: dict | None) -> Counts:
        data = data or {}
        return Counts(**{key: int(data.get(key, 0)) for key in ("tests", "failures", "errors", "skipped", "classes")})


def _first_line(text: str | None) -> str:
    return (text or "").strip().splitlines()[0][:300] if (text or "").strip() else ""


def _status_of(testcase: ElementTree.Element) -> tuple[str, str]:
    for tag, status in (("failure", "failure"), ("error", "error"), ("skipped", "skipped")):
        element = testcase.find(tag)
        if element is not None:
            return status, _first_line(element.get("message") or element.text)
    return "passed", ""


def parse_xml(path: Path) -> list[TestResult]:
    """Return the test cases of a JUnit-format file, or nothing if it isn't one or is still being written."""
    try:
        root = ElementTree.parse(path).getroot()
    except (ElementTree.ParseError, OSError):
        return []
    if root.tag not in ("testsuite", "testsuites"):
        return []
    suite = root.get("name", "") if root.tag == "testsuite" else ""
    results = []
    for testcase in root.iter("testcase"):
        status, message = _status_of(testcase)
        results.append(TestResult(testcase.get("classname", ""), testcase.get("name", ""), status, message, suite))
    return results


def parse_events(path: Path) -> list[TestResult]:
    """Return the tests of an events file with one status, class, method line per test."""
    results = []
    try:
        lines = path.read_text(errors="replace").splitlines()
    except OSError:
        return []
    for line in lines:
        fields = line.split("\t")
        if len(fields) >= 3 and fields[0] in STATUSES:
            results.append(TestResult(fields[1], fields[2], fields[0]))
    return results


def parse_results(paths: list[Path]) -> list[TestResult]:
    results = []
    for path in paths:
        results += parse_xml(path) if path.suffix == ".xml" else parse_events(path)
    return results


def class_seconds(paths: list[Path]) -> dict[str, float]:
    """Return the summed test case time of each class in JUnit-format files."""
    seconds: dict[str, float] = {}
    for path in paths:
        try:
            root = ElementTree.parse(path).getroot()
        except (ElementTree.ParseError, OSError):
            continue
        for testcase in root.iter("testcase"):
            try:
                time = float(testcase.get("time") or 0)
            except ValueError:
                continue
            classname = testcase.get("classname", "")
            seconds[classname] = seconds.get(classname, 0.0) + time
    return seconds


def find_xml(root: Path) -> list[Path]:
    return sorted(root.rglob("*.xml")) if root.is_dir() else []


def count(results: list[TestResult]) -> Counts:
    return Counts(
        tests=len(results),
        failures=sum(1 for result in results if result.status == "failure"),
        errors=sum(1 for result in results if result.status == "error"),
        skipped=sum(1 for result in results if result.status == "skipped"),
        classes=len({result.suite or result.classname for result in results}),
    )


def failing(results: list[TestResult]) -> list[TestResult]:
    return [result for result in results if result.status in ("failure", "error")]
