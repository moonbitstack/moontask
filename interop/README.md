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

**Direction B, green.** A real Celery 5.5.3 worker on the `celery` queue consumed
an envelope this library published and answered it:

```
published moon-1793966175696 t.double(21)
celery answered 42
```

`Handle::get` read the answer back out of `celery-task-meta-<id>`, so the
publish side, the result side and the id all agree with Celery.

**Direction A, not green yet.** Celery publishes (verified: one envelope on the
queue, `state right after publishing: PENDING`), the bridge reads it — but the
first run died on our own parser (`malformed: missing key id`): `consume` was
still calling `Message::parse` on a Celery envelope. That branch is now written
(`Message::from_celery` when the broker is in Celery's layout, `@json.parse` of
the payload first) and the module builds, but the run that would have shown the
`SUCCESS` meta did not finish inside the harness timeout, so the claim is not
made.

## What is not done

- Direction A's green run.
- The differential on state sequences and retry intervals.
- Two-way *chains* and chords: the envelope carries `chain` and `chord` fields
  that this codec writes as empty.

`setup.sh` builds the venv (`celery==5.5.3`, kombu 5.5.4) and starts redis;
`run_interop.sh` drives both directions, one-shot and bounded.
