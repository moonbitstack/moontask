"""Record the homepage figure's run, so the page replays what the library did.

`examples/05-floor` drives a fixed scenario on a hand-moved clock and prints one
JSON object per event; this runs it and writes the list to `docs/assets/floor.json`.
The page never invents an event, and CI re-runs this and fails if the file and the
library have drifted apart.

Usage: python scripts/gen_floor.py
"""

import json
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
TARGET = ROOT / "docs" / "assets" / "floor.json"
FIGURE = ROOT / "docs" / "figures" / "floor-data.js"


def main():
    run = subprocess.run(
        ["moon", "run", "src/examples/05-floor", "--target", "native"],
        cwd=ROOT,
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace",
    )
    if run.returncode != 0:
        print(run.stdout + run.stderr, file=sys.stderr)
        return 1
    events = []
    for line in run.stdout.splitlines():
        line = line.strip()
        if not line.startswith("{"):
            continue
        events.append(json.loads(line))
    if not events:
        print("the example printed no events", file=sys.stderr)
        return 1
    # A card's id carries the clock it was made on; the page only needs order and
    # identity, so the ids are handed over as they are and never parsed there.
    seen = {}
    for event in events:
        seen.setdefault(event["id"], len(seen) + 1)
        event["card"] = seen[event["id"]]
    payload = {
        "note": "recorded from examples/05-floor; do not edit by hand",
        "events": events,
    }
    TARGET.parent.mkdir(parents=True, exist_ok=True)
    TARGET.write_text(
        json.dumps(payload, indent=2, ensure_ascii=False) + "\n",
        encoding="utf-8",
        newline="\n",
    )
    # The figure loads this beside itself: a page opened from disk cannot fetch a
    # sibling file, and a figure that needs a server to draw is a figure that does
    # not draw where people look at it.
    FIGURE.write_text(
        "window.FLOOR = " + json.dumps(payload, ensure_ascii=False) + ";\n",
        encoding="utf-8",
        newline="\n",
    )
    print(f"{TARGET.relative_to(ROOT).as_posix()}: {len(events)} events")
    print(f"{FIGURE.relative_to(ROOT).as_posix()}: the same, for the figure")
    return 0


if __name__ == "__main__":
    sys.exit(main())
