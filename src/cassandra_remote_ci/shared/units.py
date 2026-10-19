# SPDX-License-Identifier: Apache-2.0
"""Durations and sizes as people read them."""


def duration(seconds: float) -> str:
    seconds = int(seconds)
    if seconds >= 3600:
        return f"{seconds // 3600}h{seconds % 3600 // 60:02d}m"
    if seconds >= 60:
        return f"{seconds // 60}m"
    return f"{seconds}s"


def gigabytes(size: float) -> str:
    return f"{size / 2**30:.1f}G"
