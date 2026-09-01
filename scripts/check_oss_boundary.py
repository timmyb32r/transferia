#!/usr/bin/env python3
"""Reject environment-specific identities and infrastructure from OSS files."""

from __future__ import annotations

import re
import subprocess
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
CHECKER = Path(__file__).resolve()
FORBIDDEN = tuple(
    re.compile(pattern, re.IGNORECASE)
    for pattern in (
        r"yandex-cloud",
        r"yandex-team",
        r"registry\.yandex",
        r"(?:^|[.])yandex\.net",
        r"timmyb32r",
        r"YT_SECURE_VAULT",
        r"//home/logfeller",
        r"mdb-junk",
        r"(?:^|[/~])\.mdb/token",
        r"arcadia_rust",
        r"(?:^|/)arcadia/",
    )
)


def tracked_files(root: Path) -> list[Path]:
    repository = Path(
        subprocess.check_output(
            ["git", "-C", root, "rev-parse", "--show-toplevel"], text=True
        ).strip()
    )
    project = root.relative_to(repository)
    output = subprocess.check_output(
        ["git", "-C", repository, "ls-files", "-z", "--", str(project)],
    )
    return [repository / entry.decode() for entry in output.split(b"\0") if entry]


def violations(paths: list[Path]) -> list[str]:
    errors: list[str] = []
    for path in paths:
        if path.resolve() == CHECKER:
            continue
        try:
            text = path.read_text(encoding="utf-8")
        except (UnicodeDecodeError, OSError):
            continue
        try:
            label = path.relative_to(ROOT)
        except ValueError:
            label = path
        for number, line in enumerate(text.splitlines(), 1):
            if any(pattern.search(line) for pattern in FORBIDDEN):
                errors.append(f"{label}:{number}: {line.strip()}")
    return errors


def main() -> int:
    errors = violations(tracked_files(ROOT))
    if errors:
        print("OSS boundary violations:\n" + "\n".join(errors))
        return 1
    print("OSS boundary OK")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
