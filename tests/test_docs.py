# Copyright 2026 Dheeraj Turaga
# SPDX-License-Identifier: Apache-2.0
"""The documentation's links: each relative link and image reaches a file, and each anchor a heading."""

from __future__ import annotations

import re
from pathlib import Path

import pytest

ROOT = Path(__file__).parents[1]
MARKDOWN = sorted([*ROOT.glob("*.md"), *(ROOT / "docs").glob("*.md")])
FENCE = re.compile(r"^```.*?^```", re.MULTILINE | re.DOTALL)
LINK = re.compile(r"\[(?:[^\[\]]|\[[^\]]*\])*\]\(([^)\s]+)\)")
IMAGE = re.compile(r'(?:src|srcset)="([^"]+)"')


def anchors(path: Path) -> set[str]:
    """Return the anchors GitHub gives a Markdown file's headings."""
    found: list[str] = []
    for heading in re.findall(r"^#{1,6} (.+)$", FENCE.sub("", path.read_text()), re.MULTILINE):
        # GitHub lowercases a heading, drops what is not a letter, digit, space, - or _, and turns
        # spaces into hyphens; a repeated heading gets -1, -2 and so on.
        slug = re.sub(r"[^\w\- ]", "", heading.strip().lower()).replace(" ", "-")
        found.append(f"{slug}-{found.count(slug)}" if slug in found else slug)
    return set(found)


def targets(path: Path) -> list[str]:
    text = FENCE.sub("", path.read_text())
    return [*LINK.findall(text), *IMAGE.findall(text)]


@pytest.mark.parametrize("path", MARKDOWN, ids=[str(path.relative_to(ROOT)) for path in MARKDOWN])
def test_relative_links_reach_a_file_and_a_heading(path):
    broken = []
    for target in targets(path):
        if re.match(r"[a-z]+:", target):
            continue
        file, _, anchor = target.partition("#")
        linked = (path.parent / file).resolve() if file else path
        if not linked.exists():
            broken.append(f"{target}: no such file")
        elif anchor and linked.suffix == ".md" and anchor not in anchors(linked):
            broken.append(f"{target}: no such heading")

    assert broken == [], f"{path.relative_to(ROOT)} has broken links: {broken}"
