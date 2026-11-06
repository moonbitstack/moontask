#!/usr/bin/env bash
# Two directions between Celery 5 and moontask, over one redis.
#
#   A  Celery publishes -> this library claims, runs, writes the meta -> Celery reads it
#   B  this library publishes -> a Celery worker runs it -> the handle reads the answer
#
# Every step is bounded and nothing waits on a process to exit: the worker and
# the bridge are killed, and what they produced is read out of redis.
set -u
HERE="$(cd "$(dirname "$0")" && pwd)"
REPO="$(cd "$HERE/.." && pwd)"
PY="${PY:-$HOME/.celery-venv/bin/python}"
BRIDGE="$REPO/_build/native/debug/build/moonbitstack/moontask-redis/example/example.exe"
R="docker exec ct-redis redis-cli"

docker rm -f ct-redis >/dev/null 2>&1
docker run -d --rm --name ct-redis -p 6379:6379 redis:7 >/dev/null
for _ in $(seq 1 20); do $R ping 2>/dev/null | grep -q PONG && break; sleep 1; done

echo "== A: Celery publishes, this library runs it"
$R flushall >/dev/null
"$PY" "$HERE/celery_side.py" publish cel-A celery
echo -n "on the queue before anyone takes it: "; $R llen celery
timeout -k 2 40 "$BRIDGE" consume > "$HERE/out-bridge.txt" 2>&1 &
BPID=$!
for _ in $(seq 1 20); do [ -n "$($R get celery-task-meta-cel-A)" ] && break; sleep 0.6; done
kill -9 $BPID 2>/dev/null; wait $BPID 2>/dev/null
echo -n "the meta a Celery client reads: "; $R get celery-task-meta-cel-A
"$PY" "$HERE/celery_side.py" result cel-A

echo
echo "== B: this library publishes, a real Celery worker runs it"
$R flushall >/dev/null
timeout -k 2 40 "$PY" "$HERE/celery_side.py" worker celery > "$HERE/out-worker.txt" 2>&1 &
WPID=$!
sleep 9
timeout -k 2 35 "$BRIDGE" publish 21
kill -9 $WPID 2>/dev/null; wait $WPID 2>/dev/null
