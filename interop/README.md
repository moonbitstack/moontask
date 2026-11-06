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

## The two directions

Both against Celery 5.5.3 and kombu 5.5.4, one redis, `bash run_interop.sh`:

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
the claim was this library's, and the answer was written where Celery looks. B
runs the other way: a real Celery worker consumed our envelope and wrote the
answer, and `Handle::get` read it back.

## The differential: one ladder, run twice

The same task — fail, wait, fail, wait, succeed — under the same policy on both
sides: two retries, a wait that doubles from half a second. Each attempt records
the millisecond it started.

```sh
moon run src/examples/03-retry > moontask-ladder.jsonl
python retry_ladder.py          > celery-ladder.jsonl     # starts its own worker
python compare_ladder.py moontask-ladder.jsonl celery-ladder.jsonl
```

```
           attempts    state  waits (ms)
moontask          3  SUCCESS  513, 1001
celery            3  SUCCESS  509, 1006

attempts equal: yes · waits within 120 ms: yes
  wait 1: moontask 513 ms · celery 509 ms · difference 4 ms
  wait 2: moontask 1001 ms · celery 1006 ms · difference 5 ms
```

Three attempts, the same final state, and a wait that differs by 4 and 5 ms —
which is the cost of measuring a timer from inside the task, not a different
policy. The transcripts are committed as `moontask-ladder.jsonl` and
`celery-ladder.jsonl`.

## What is not done

- Chains and chords across the boundary: the envelope has `chain` and `chord`
  fields, and this codec writes them empty.
- A Celery worker's reservation lives in one `unacked` hash for every consumer;
  this broker keeps its own in-flight lists. Two of our workers can share a queue
  with a Celery worker, but a Celery worker that dies leaves its reservation
  where our `recover` does not look.
- ETA messages: Celery's worker holds a delayed message in memory; ours parks it
  in a sorted set of its own, outside the list Celery reads.

`setup.sh` builds the venv (`celery==5.5.3`, kombu 5.5.4) and starts redis.
`celery_side.py` is a real Celery app with the same two task names.
