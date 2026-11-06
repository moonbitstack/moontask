#!/usr/bin/env bash
# Two directions between Celery 5 and moontask, over one redis.
#   A  Celery publishes -> this library claims, runs, writes the meta
#   B  this library publishes -> a Celery worker runs it -> the handle reads it
# Everything is one-shot and bounded: no worker is left running.
set -u
HERE="$(cd "$(dirname "$0")" && pwd)"
REPO="$(cd "$HERE/.." && pwd)"
PY="${PY:-$HOME/.celery-venv/bin/python}"
BRIDGE="$REPO/_build/native/debug/build/moonbitstack/moontask-redis/example/example.exe"
R="docker exec ct-redis redis-cli"

echo "== A: Celery publishes, this library runs it"
$R flushall >/dev/null
"$PY" "$HERE/celery_side.py" publish cel-A celery
echo -n "on the queue: "; $R llen celery
timeout 20 "$BRIDGE" consume
echo -n "result Celery would read: "; $R get celery-task-meta-cel-A
echo

echo "== B: this library publishes, a Celery worker runs it"
$R flushall >/dev/null
"$PY" "$HERE/celery_side.py" worker celery > "$HERE/out-worker.txt" 2>&1 &
WPID=$!
sleep 9
timeout 30 "$BRIDGE" publish 21
kill $WPID 2>/dev/null
sleep 1
