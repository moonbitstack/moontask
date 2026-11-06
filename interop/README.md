# Interop with Celery 5

What is here is the *evidence*, not a finished bridge.

`celery_side.py` is a real Celery 5.5.3 app (`t.double`, `t.add`) against a real
redis. Running `t.double.apply_async((21,), task_id="probe-1")` through it and
reading the queue with `redis-cli lrange celery 0 0` gives the envelope this
library's codec is written against — one JSON object:

```json
{
  "body": "W1syMSwge30sIHsiY2FsbGJhY2tzIjogbnVsbCwgImVycmJhY2tzIjogbnVsbCwgImNoYWluIjogbnVsbCwgImNob3JkIjogbnVsbH1d",
  "content-encoding": "utf-8",
  "content-type": "application/json",
  "headers": {
    "lang": "py", "task": "t.double", "id": "probe-1", "retries": 0,
    "eta": null, "expires": null, "root_id": "probe-1", "parent_id": null,
    "timelimit": [null, null], "argsrepr": "(21,)", "kwargsrepr": "{}",
    "origin": "gen1741@LAPTOP...", "ignore_result": false
  },
  "properties": {
    "correlation_id": "probe-1", "delivery_mode": 2,
    "delivery_info": {"exchange": "", "routing_key": "celery"},
    "priority": 0, "body_encoding": "base64", "delivery_tag": "..."
  }
}
```

The body decodes to `[[21], {}, {callbacks, errbacks, chain, chord}]`.

## What is done

- `@spec.Message::to_celery` / `from_celery` build and read exactly that, key for
  key, and `@spec.Outcome::to_celery` writes what `celery-task-meta-<id>` holds.
  Five tests in `src/spec/celery_test.mbt` cover the round trip, the retry count,
  the eta and the result meta.
- `@redis.Broker::celery` reads and writes the queue Celery uses — the list named
  after the queue, with no prefix — while keeping its delay set and in-flight
  lists under `moontask`. It compiles and is used by nothing yet.

## What has been run

Both directions, against Celery 5.5.3 and kombu 5.5.4, one redis, `run_interop.sh`:

```
== A: Celery publishes, this library runs it
celery published cel-A t.double(21) on celery
state right after publishing: PENDING
on the queue before anyone takes it: 1
the meta a Celery client reads: {"task_id":"cel-A","status":"SUCCESS","result":42,...}
celery sees cel-A: SUCCESS 42

== B: this library publishes, a real Celery worker runs it
published moon-1793973395090 t.double(21)
celery answered 42
```

A is the hard direction: `AsyncResult("cel-A").get()` returned 42 for a run this
library executed — the queue was Celery's list, the bytes were Celery's envelope,
the claim was this library's, and the answer was written where Celery looks.
B runs the other way: a real Celery worker consumed our envelope and wrote the
answer, and `Handle::get` read it back.

## What is not done

- The differential on state sequences and retry intervals. What is proven is that
  one message crosses in each direction and produces the same value on both
  sides; a task that fails, retries on a backoff and settles has not been
  compared run for run.
- Chains and chords across the boundary: the envelope has `chain` and `chord`
  fields, and this codec writes them empty.
- A Celery worker's reservation lives in one `unacked` hash for every consumer;
  this broker keeps its own in-flight lists. Two of our workers share a queue
  with a Celery worker, but a Celery worker that dies leaves its reservation
  where our `recover` does not look.

`setup.sh` builds the venv (`celery==5.5.3`, kombu 5.5.4) and starts redis.
`celery_side.py` is a real Celery app with the same two task names.
