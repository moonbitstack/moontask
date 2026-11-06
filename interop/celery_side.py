"""The Celery 5 side of the interop test.

Two tasks, the same names the MoonBit side registers, and enough subcommands to
drive both directions from `run_interop.sh`:

    publish  <id> <queue>   put t.double(21) on the queue with Celery's producer
    result   <id>           read the answer the way a Celery client does
    worker   <queue>        run a real Celery worker, one task at a time
    envelope <id>           print the envelope Celery wrote, for the diff
"""

import json
import sys
import time

from celery import Celery

BROKER = "redis://127.0.0.1:6379/0"

app = Celery("moontask_interop", broker=BROKER, backend=BROKER)
app.conf.update(
    task_serializer="json",
    result_serializer="json",
    accept_content=["json"],
    task_default_queue="celery",
    worker_prefetch_multiplier=1,
    task_acks_late=False,
)


@app.task(name="t.double")
def double(value):
    return value * 2


@app.task(name="t.add")
def add(a, b):
    return a + b


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

    if command == "add":
        # A task with two positional arguments, which this library carries as the
        # tuple: on the MoonBit side `t.add` takes `Array[Int]`.
        task_id = argv[2] if len(argv) > 2 else "cel-add"
        add.apply_async((2, 3), task_id=task_id)
        print(f"celery published {task_id} t.add(2, 3)")
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
        queue = argv[2] if len(argv) > 2 else "celery"
        app.worker_main(["worker", "-Q", queue, "-c", "1", "-l", "warning"])
        return

    if command == "envelope":
        task_id = argv[2] if len(argv) > 2 else "cel-1"
        from redis import Redis

        raw = Redis.from_url(BROKER).lrange("celery", 0, -1)
        for item in raw:
            envelope = json.loads(item)
            if envelope["headers"].get("id") == task_id:
                print(json.dumps(envelope, indent=2, sort_keys=True))
                return
        print("not on the queue")
        return

    raise SystemExit(f"unknown command {command}")


if __name__ == "__main__":
    main(sys.argv)
