#!/usr/bin/env bash
# The links between moontask and a real Celery 5 over one redis: a chain, a
# chord, a reservation handed back, and an eta — each in both directions.
#
#   bash run_links.sh <scenario>
#
# Every scenario starts from an empty server and a worker of its own, prints what
# each side ran, and leaves nothing behind.
set -u
HERE="$(cd "$(dirname "$0")" && pwd)"
REPO="$(cd "$HERE/.." && pwd)"
PY="${PY:-$HOME/.celery-venv/bin/python}"
BRIDGE="$REPO/_build/native/debug/build/moonbitstack/moontask-redis/example/example.exe"
R="docker exec ct-redis redis-cli"

say() { echo "-- $*"; }

setup() {
  # A container that is being torn down still holds the port for a moment, so
  # the start is retried rather than racing it.
  docker rm -f ct-redis >/dev/null 2>&1
  for _ in $(seq 1 10); do
    docker run -d --rm --name ct-redis -p 6379:6379 redis:7 >/dev/null 2>&1 && break
    sleep 1
  done
  for _ in $(seq 1 30); do $R ping 2>/dev/null | grep -q PONG && break; sleep 1; done
  $R flushall >/dev/null
  sleep 1
}

# A Celery worker on the given queues; its pid is echoed on stdout.
worker() {
  "$PY" "$HERE/celery_side.py" worker "$@" >"$HERE/.worker.log" 2>&1 &
  echo $!
}

case "${1:-all}" in

chain-out)
  # A chain published here, run by a Celery worker: the second link is called
  # with the first one's result, which is Celery's rule for a chain.
  setup
  say "a Celery worker on chainA,chainB"
  WPID=$(worker chainA,chainB)
  sleep 9
  say "moontask publishes t.step1(5) -> t.step2 carrying the chain"
  "$BRIDGE" chain chainA chainB
  sleep 6
  kill "$WPID" 2>/dev/null; wait "$WPID" 2>/dev/null
  say "what ran"
  "$PY" "$HERE/celery_side.py" log
  ;;

chain-in)
  # A chain published by Celery, whose head this library runs and whose tail a
  # Celery worker runs: the head's result has to be prepended here.
  setup
  say "a Celery worker on chainB only"
  WPID=$(worker chainB)
  sleep 9
  say "celery publishes chain(t.step1(5), t.step2())"
  "$PY" "$HERE/celery_side.py" chain chainA chainB
  say "moontask runs the head"
  "$BRIDGE" consume chainA 1 15
  sleep 5
  kill "$WPID" 2>/dev/null; wait "$WPID" 2>/dev/null
  say "what ran"
  "$PY" "$HERE/celery_side.py" log
  ;;

chord-out)
  # A chord built here: the parts run here, so this side writes the join, and
  # the callback runs on a Celery worker.
  setup
  say "a Celery worker on cbq"
  WPID=$(worker cbq)
  sleep 9
  say "moontask publishes a chord of two parts on partq, callback on cbq"
  "$BRIDGE" chord partq cbq
  say "moontask runs both parts"
  "$BRIDGE" consume partq 2 20
  sleep 6
  kill "$WPID" 2>/dev/null; wait "$WPID" 2>/dev/null
  say "what ran"
  "$PY" "$HERE/celery_side.py" log
  ;;

chord-in)
  # A chord built by Celery, whose parts are split: one runs on a Celery worker
  # and one here, and this side's part is the one that closes the join.
  setup
  say "a Celery worker on partB,cbq"
  WPID=$(worker partB,cbq)
  sleep 9
  say "celery publishes chord(t.step1(1) on partA, t.step1(2) on partB)(t.sum)"
  "$PY" "$HERE/celery_side.py" chord partA partB cbq
  say "the Celery part runs first"
  COUNT=0
  for _ in $(seq 1 25); do
    COUNT=$("$PY" "$HERE/celery_side.py" log | grep -c step1 || true)
    [ "$COUNT" -ge 1 ] && break
    sleep 1
  done
  say "parts in so far: $COUNT"
  say "moontask runs the other part and joins"
  "$BRIDGE" consume partA 1 20
  sleep 6
  kill "$WPID" 2>/dev/null; wait "$WPID" 2>/dev/null
  say "what ran"
  "$PY" "$HERE/celery_side.py" log
  ;;

recover)
  # A Celery worker killed while holding a message: the reservation is still in
  # Celery's table, and `recover` here is what hands it back.
  setup
  say "a Celery worker on slowq, running t.slow(30)"
  WPID=$(worker slowq)
  sleep 9
  "$PY" "$HERE/celery_side.py" slow 30 slowq
  sleep 4
  say "killing it while it works"
  kill -9 "$WPID" 2>/dev/null; wait "$WPID" 2>/dev/null
  say "the reservation it left:"
  $R hlen unacked
  sleep 3
  say "moontask recovers with a 2 s visibility"
  "$BRIDGE" recover 2
  say "on the queue now:"
  $R llen slowq
  say "a fresh worker takes it again"
  WPID=$(worker slowq)
  sleep 10
  kill "$WPID" 2>/dev/null; wait "$WPID" 2>/dev/null
  "$PY" "$HERE/celery_side.py" log
  ;;

restore)
  # The other way round: this library claims a message and dies, and a Celery
  # worker's own visibility timeout puts it back.
  #
  # The worker is started after the claim has aged past the visibility timeout:
  # Celery checks once every ten polls, so a worker that was already running
  # would only look about two minutes later.
  setup
  say "celery publishes t.double(21) on resq"
  "$PY" "$HERE/celery_side.py" publish res-1 resq
  say "moontask claims it and never settles it"
  "$BRIDGE" claim resq
  say "the reservation is in Celery's table:"
  $R hlen unacked
  say "waiting 6 s so the claim is past a 5 s visibility"
  sleep 6
  say "a Celery worker on resq with a 5 s visibility"
  WPID=$(worker resq 5)
  sleep 15
  kill "$WPID" 2>/dev/null; wait "$WPID" 2>/dev/null
  say "what ran"
  "$PY" "$HERE/celery_side.py" log
  say "the reservation is gone:"
  $R hlen unacked
  ;;

eta-out)
  # An eta published here: Celery's client puts such a message on the queue and
  # lets the worker hold it until the eta, so ours has to be on the queue too —
  # this library's own layout would have parked it in a sorted set instead.
  setup
  say "moontask publishes t.step1(5) due in 4 s, with no worker running"
  "$BRIDGE" eta etaq 4
  say "on the queue right after publishing:"
  $R llen etaq
  say "a Celery worker on etaq"
  WPID=$(worker etaq)
  sleep 14
  kill "$WPID" 2>/dev/null; wait "$WPID" 2>/dev/null
  say "what ran:"
  "$PY" "$HERE/celery_side.py" log
  say "compare the run's at with the due above"
  ;;

eta-in)
  # An eta published by Celery: it is on the queue straight away, and the
  # consumer here has to hold it rather than run it early.
  setup
  say "celery publishes t.step1(5) three seconds out"
  "$PY" "$HERE/celery_side.py" eta etaq 3
  say "on the queue right after publishing:"
  $R llen etaq
  say "moontask consumes etaq, and must wait for the eta"
  "$BRIDGE" consume etaq 1 15
  say "compare the run's at with the due above"
  ;;

all)
  for one in chain-out chain-in chord-out chord-in recover restore eta-out eta-in; do
    echo
    echo "================ $one"
    bash "$0" "$one"
  done
  ;;

*)
  echo "unknown scenario $1" >&2
  exit 1
  ;;
esac
