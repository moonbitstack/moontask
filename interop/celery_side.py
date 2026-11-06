"""The Celery 5 side of the interop test.

Every task appends one JSON line to a redis list, so a transcript can be read
back after a worker is killed: what ran, with what arguments, at what time.

    worker   <queues> [visibility]  run a real Celery worker, one task at a time
    publish  <id> <queue>           put t.double(21) on the queue
    result   <id>                   read the answer the way a Celery client does
    chain    <q1> <q2>              publish a two-link chain, head on q1
    chord    <qa> <qb> <qcb>        publish a chord whose parts are on qa and qb
    eta      <queue> <seconds>      publish t.step1(5) with a countdown
    slow     <seconds> <queue>      publish t.slow, which sleeps
    log [name]                      print what the tasks recorded
    envelope <id>                   print the envelope Celery wrote
"""

import json
import os
import sys
import time

from celery import Celery, chord as chord_of
from redis import Redis

BROKER = "redis://127.0.0.1:6379/0"
LOG_KEY = "log:celery"
redis = Redis.from_url(BROKER)

app = Celery("moontask_interop", broker=BROKER, backend=BROKER)
app.conf.update(
    task_serializer="json",
    result_serializer="json",
    accept_content=["json"],
    task_default_queue="celery",
    worker_prefetch_multiplier=1,
    task_acks_late=True,
)


def record(name, args):
    redis.rpush(
        LOG_KEY,
        json.dumps(
            {"task": name, "args": args, "at": round(time.time() * 1000), "pid": os.getpid()}
        ),
    )


@app.task(name="t.double")
def double(value):
    record("t.double", [value])
    return value * 2


@app.task(name="t.add")
def add(a, b):
    record("t.add", [a, b])
    return a + b


@app.task(name="t.step1")
def step1(value):
    record("t.step1", [value])
    return value * 10


@app.task(name="t.step2")
def step2(value):
    record("t.step2", [value])
    return value * 10


@app.task(name="t.sum")
def total(values):
    record("t.sum", [values])
    return sum(values)


@app.task(name="t.echo")
def echo(value):
    record("t.echo", [value])
    return value


@app.task(name="t.slow")
def slow(seconds):
    record("t.slow", [seconds])
    time.sleep(seconds)
    return "done"


def main(argv):
    if len(argv) < 2:
        raise SystemExit(__doc__)
    command = argv[1]

    if command == "publish":
        task_id = argv[2] if len(argv) > 2 else "cel-1"
        queue = argv[3] if len(argv) > 3 else "celery"
        result = double.apply_async((21,), task_id=task_id, queue=queue)
        print(f"celery published {task_id} t.double(21) on {queue}")
        print(f"state right after publishing: {result.state}")
        return

    if command == "result":
        task_id = argv[2] if len(argv) > 2 else "cel-1"
        result = double.AsyncResult(task_id)
        deadline = time.time() + 20
        while time.time() < deadline:
            if result.ready():
                break
            time.sleep(0.05)
        print(f"celery sees {task_id}: {result.state} {result.result!r}")
        return

    if command == "worker":
        queues = argv[2] if len(argv) > 2 else "celery"
        if len(argv) > 3:
            # How long a claim may stay unsettled before the worker hands it
            # back; the default is an hour, which no test can wait for.
            app.conf.broker_transport_options = {"visibility_timeout": int(argv[3])}
        # `-P solo`: one process, so `kill -9` really leaves nothing behind and
        # the reservation it was holding is what the next step finds.
        app.worker_main(["worker", "-Q", queues, "-c", "1", "-P", "solo", "-l", "warning"])
        return

    if command == "chain":
        q1 = argv[2]
        q2 = argv[3]
        # Celery's chain: the head is published, the tail rides in its body, and
        # the tail is called with the head's result prepended.
        result = (step1.s(5).set(queue=q1) | step2.s().set(queue=q2)).apply_async()
        print(f"celery published chain {result.id or 'group'}")
        return

    if command == "chord":
        qa, qb, qcb = argv[2], argv[3], argv[4]
        result = chord_of(
            [step1.s(1).set(queue=qa), step1.s(2).set(queue=qb)]
        )(total.s().set(queue=qcb))
        print(f"celery published chord {result.id}")
        return

    if command == "eta":
        queue = argv[2] if len(argv) > 2 else "celery"
        seconds = int(argv[3]) if len(argv) > 3 else 3
        result = step1.apply_async((5,), countdown=seconds, queue=queue)
        due = round((time.time() + seconds) * 1000)
        print(f"celery published {result.id} due at {due} ({seconds}s out)")
        return

    if command == "slow":
        seconds = int(argv[2]) if len(argv) > 2 else 20
        queue = argv[3] if len(argv) > 3 else "celery"
        result = slow.apply_async((seconds,), queue=queue)
        print(f"celery published slow({seconds}) as {result.id}")
        return

    if command == "log":
        for raw in redis.lrange(LOG_KEY, 0, -1):
            print(raw.decode())
        return

    if command == "envelope":
        task_id = argv[2] if len(argv) > 2 else "cel-1"
        for item in redis.lrange("celery", 0, -1):
            envelope = json.loads(item)
            if envelope["headers"].get("id") == task_id:
                print(json.dumps(envelope, indent=2, sort_keys=True))
                return
        print("not on the queue")
        return

    raise SystemExit(f"unknown command {command}")


if __name__ == "__main__":
    main(sys.argv)
