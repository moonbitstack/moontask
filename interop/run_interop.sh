#!/usr/bin/env bash
# Two directions between Celery 5.5 and moontask, over one redis.
#
#   A  Celery publishes -> a moontask worker runs it -> Celery reads the answer
#   B  moontask publishes -> a Celery worker runs it -> moontask reads the answer
#
# Run from anywhere with the repository mounted; needs the venv built by
# `setup.sh` and a redis on 127.0.0.1:6379.
set -u
HERE="$(cd "$(dirname "$0")" && pwd)"
REPO="$(cd "$HERE/.." && pwd)"
PY="${PY:-$HOME/.celery-venv/bin/python}"
MOON="${MOON:-$HOME/.moon/bin/moon}"
BRIDGE="$REPO/_build/native/debug/build/moonbitstack/moontask-redis/example/example.exe"
R="docker exec ct-redis redis-cli"

flush() { $R flushall >/dev/null; }

echo "== build the bridge"
(cd "$REPO/moontask-redis" && "$MOON" build --target native >/dev/null) || exit 1
[ -x "$BRIDGE" ] || { echo "no bridge at $BRIDGE"; exit 1; }

echo
echo "== A: Celery publishes, a moontask worker runs it"
flush
"$PY" "$HERE/celery_side.py" publish cel-A celery
echo "-- what is on the queue before anyone consumes it:"
$R llen celery
"$BRIDGE" consume > "$HERE/out-consume.txt" 2>&1 &
BRIDGE_PID=$!
sleep 6
kill $BRIDGE_PID 2>/dev/null; wait $BRIDGE_PID 2>/dev/null
cat "$HERE/out-consume.txt"
"$PY" "$HERE/celery_side.py" result cel-A

echo
echo "== B: moontask publishes, a real Celery worker runs it"
flush
"$PY" "$HERE/celery_side.py" worker celery > "$HERE/out-worker.txt" 2>&1 &
WORKER_PID=$!
sleep 8
"$BRIDGE" publish 21
kill $WORKER_PID 2>/dev/null; wait $WORKER_PID 2>/dev/null

echo
echo "== the queue is Celery's: no key of ours holds the ready message"
$R keys 'celery' | head -3
$R keys 'moontask:*' | head -5
