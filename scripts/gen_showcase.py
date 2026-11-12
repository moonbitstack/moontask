#!/usr/bin/env python3
"""Record what the examples print, for the showcase page to replay.

The page must show the library running, not a hand-written impression of it, so
the lines it animates come from running the examples that ship in the module.
Each example pins its clock, so the recording is the same on every machine and
the workflow can check it has not drifted.

Run from the repository root:

    python3 scripts/gen_showcase.py
"""

from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
EXAMPLES = [
    ("queue", "src/examples/01-queue", "A queue end to end"),
    ("canvas", "src/examples/02-canvas", "A chain, a group and a chord"),
    ("fanout", "src/examples/07-fanout", "A map, a starmap and a chunk"),
]
HEADER = """// Written by scripts/gen_showcase.py from a run of the examples. Do not edit.
window.TRANSCRIPTS = """
FOOTER = ";\n"


def record(path: str) -> list[str]:
    result = subprocess.run(
        ["moon", "run", path, "--target", "native"],
        cwd=ROOT,
        capture_output=True,
        text=True,
    )
    if result.returncode != 0:
        sys.stderr.write(result.stdout)
        sys.stderr.write(result.stderr)
        raise SystemExit(f"recording {path} failed")
    return [line for line in result.stdout.split("\n") if line.strip()]


def main() -> int:
    recordings = {}
    for name, path, title in EXAMPLES:
        recordings[name] = {"title": title, "command": f"moon run {path}", "lines": record(path)}
    target = ROOT / "docs" / "assets" / "transcripts.js"
    target.write_text(
        HEADER + json.dumps(recordings, indent=2) + FOOTER,
        encoding="utf-8",
        newline="\n",
    )
    total = sum(len(value["lines"]) for value in recordings.values())
    print(f"{total} lines recorded from {len(recordings)} examples -> docs/assets/transcripts.js")
    return 0


if __name__ == "__main__":
    sys.exit(main())
