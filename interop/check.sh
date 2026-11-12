#!/usr/bin/env bash
# The eight scenarios, each with the line it has to print for the run to count.
#
# `run_links.sh` shows what each side ran; this decides whether what it shows is
# what the claim was. A scenario that prints the wrong task, the wrong argument
# or nothing at all fails here, which is the difference between a harness that
# runs and a harness that proves something.
#
#   bash check.sh            every scenario
#   bash check.sh chain-out  one of them
set -u
HERE="$(cd "$(dirname "$0")" && pwd)"
ONLY="${1:-}"
FAILED=0

# check <scenario> <pattern>... — run it, keep its output, and require every
# pattern in it. One run can be asked several questions, which is cheaper than
# starting a worker per question.
check() {
  scenario="$1"
  shift
  if [ -n "$ONLY" ] && [ "$ONLY" != "$scenario" ]; then return 0; fi
  out="$HERE/.out-$scenario.txt"
  echo "================ check $scenario"
  bash "$HERE/run_links.sh" "$scenario" > "$out" 2>&1
  for pattern in "$@"; do
    if grep -qF "$pattern" "$out"; then
      echo "ok   $scenario: $pattern"
    else
      echo "FAIL $scenario: nothing printed '$pattern'"
      echo "---- what it printed"
      cat "$out"
      FAILED=1
      break
    fi
  done
  rm -f "$out"
}

# A chain built here is finished by a worker there, with the head's result as the
# tail's only argument.
check chain-out '"task": "t.step2", "args": [50]'
# The head runs here, the tail there, and the tail is called with the head's
# result prepended — the rule a chain link follows.
check chain-in '"task": "t.step2", "args": [50]'
# The parts run here, the join is written in Celery's keys, and the callback gets
# the parts' results in index order.
check chord-out '"task": "t.sum", "args": [[10, 20]]'
# Celery builds the chord, one part runs on each side, and this side closes it.
check chord-in '"task": "t.sum", "args": [[10, 20]]'
# A map and a starmap published here, run there: every item once, and a two-item
# tuple spread into the function's two arguments.
check fanout-out '"task": "t.add", "args": [3, 4]' '"task": "t.add", "args": [1, 2]' '"task": "t.step1", "args": [3]'
# Celery's group and starmap, run here: the list of two is the argument list and
# the single argument is that argument.
check fanout-in 't.add([10,20])' 't.add([30,40])' 't.step1(5)'
# A worker killed mid-task leaves a reservation this library hands back, and the
# task runs again: two entries, one from each worker.
check recover '"task": "t.slow", "args": [30]'
# A claim made here is restored by Celery's visibility timeout.
check restore '"task": "t.double", "args": [21]'
# An eta published here is on the queue, and Celery holds it until it is due.
check eta-out '"task": "t.step1", "args": [5]'
# An eta published by Celery is held here rather than run early.
check eta-in '"ran":1'

if [ "$FAILED" -ne 0 ]; then
  echo "at least one scenario did not do what it claims" >&2
  exit 1
fi
echo "every scenario did what it claims"
