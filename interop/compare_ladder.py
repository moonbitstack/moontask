"""Put the two retry transcripts side by side.

    python compare_ladder.py moontask.jsonl celery.jsonl

Prints the attempts each side made, the wait between them, and whether the two
agree on the count and on each gap within `TOLERANCE` milliseconds.
"""
import json
import sys

TOLERANCE = 120


def load(path):
    attempts, state = [], None
    with open(path, encoding="utf-8") as handle:
        for line in handle:
            line = line.strip()
            if not line:
                continue
            entry = json.loads(line)
            if "attempt" in entry:
                attempts.append((entry["attempt"], entry["at"]))
            else:
                state = entry.get("state")
    attempts.sort(key=lambda a: a[0])
    return {"attempts": attempts, "state": state}


def gaps(attempts):
    return [attempts[i][1] - attempts[i - 1][1] for i in range(1, len(attempts))]


def main(argv):
    left, right = load(argv[1]), load(argv[2])
    print(f"{'':10} {'attempts':>8} {'state':>8}  waits (ms)")
    for name, side in (("moontask", left), ("celery", right)):
        waits = ", ".join(str(g) for g in gaps(side["attempts"])) or "-"
        print(f"{name:10} {len(side['attempts']):>8} {str(side['state']):>8}  {waits}")

    same_count = len(left["attempts"]) == len(right["attempts"])
    left_gaps, right_gaps = gaps(left["attempts"]), gaps(right["attempts"])
    if same_count and len(left_gaps) == len(right_gaps):
        off = [abs(a - b) for a, b in zip(left_gaps, right_gaps)]
        ok = all(d <= TOLERANCE for d in off)
        print(f"\nattempts equal: yes · waits within {TOLERANCE} ms: {'yes' if ok else 'no'}")
        for i, (a, b) in enumerate(zip(left_gaps, right_gaps), start=1):
            print(f"  wait {i}: moontask {a} ms · celery {b} ms · difference {abs(a-b)} ms")
        return 0 if ok else 1
    print("\nattempts equal: no")
    return 1


if __name__ == "__main__":
    sys.exit(main(sys.argv))
