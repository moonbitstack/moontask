# Interop with Celery 5

The harness behind every claim this library makes about sharing a queue with
Celery. Nothing here is a mock: a real `celery==5.5.3` (kombu 5.5.4) runs as a
worker, a real redis holds the queue, and the MoonBit side is the example that
ships in `moontask-redis`.

```sh
bash setup.sh          # a redis, and a venv with celery[redis]==5.5.3
bash run_links.sh all  # the eight scenarios below
```

Each scenario starts from an empty server, and prints what each side actually
ran, from one log the tasks write to as they start:

```
{"task": "t.step2", "args": [50], "at": 1793976468287, "pid": 14211}
```

## One run, eight scenarios

```
================ chain-out        a chain published here, run by Celery
{"published":"chain-a-1793976468266"}
{"task": "t.step1", "args": [5], "at": 1793976468274, "pid": 14211}
{"task": "t.step2", "args": [50], "at": 1793976468287, "pid": 14211}

================ chain-in         a chain published by Celery, head run here
{"ran":1,"tasks":["t.step1@1793976486567"]}
{"task": "t.step2", "args": [50], "at": 1793976486572, "pid": 14453}

================ chord-out        a chord of parts run here, callback on Celery
{"published":"1793976503711:3:t.sum"}
{"ran":2,"tasks":["t.step1@1793976503818","t.step1@1793976503824"]}
{"task": "t.sum", "args": [[10, 20]], "at": 1793976503830, "pid": 14697}

================ chord-in         a chord split across the two, joined here
-- parts in so far: 1
{"ran":1,"tasks":["t.step1@1793976523367"]}
{"task": "t.step1", "args": [2], "at": 1793976522301, "pid": 14941}
{"task": "t.sum", "args": [[10, 20]], "at": 1793976523379, "pid": 14941}

================ recover          a Celery worker killed mid-task, recovered here
-- the reservation it left: 1
{"moved":1}
-- on the queue now: 1
{"task": "t.slow", "args": [30], "at": 1793976541614, "pid": 15188}
{"task": "t.slow", "args": [30], "at": 1793976550291, "pid": 15263}

================ restore          a claim made here, handed back by Celery
{"claimed":"moontask-1793976562188:1793976562189:1"}
-- the reservation is in Celery's table: 1
{"task": "t.double", "args": [21], "at": 1793976569661, "pid": 15528}
-- the reservation is gone: 0

================ eta-out           an eta published here, honoured by Celery
{"published":"eta-1793976586446","due":1793976590446}
-- on the queue right after publishing: 1
{"task": "t.step1", "args": [5], "at": 1793976590448, "pid": 15816}

================ eta-in            an eta published by Celery, honoured here
celery published 1ce5a6f4-… due at 1793976606754 (3s out)
-- on the queue right after publishing: 1
{"ran":1,"tasks":["t.step1@1793976606746"]}
```

## What each one shows

**chain-out.** A chain built here carries its tail in the first message's body
(`embed.chain`), exactly as Celery does. The Celery worker runs the head, then
applies the tail with the head's result prepended: `t.step1(5)` returns 50 and
`t.step2` is called with 50. That is Celery's rule, and the harness shows it
working on a chain this library wrote.

**chain-in.** The same thing backwards. Celery publishes
`chain(t.step1.s(5), t.step2.s())`, this library runs the head, and because the
tail is not `immutable` it is published with `[50]` as its argument, which the
Celery worker on `chainB` then runs.

**chord-out.** Two parts published here, on a queue this side consumes, with the
callback on a queue the Celery worker owns. Each part's result is written into
Celery's join key as it settles (`.j`, scored by the part's index), and the part
that brings the count up to `.s` publishes the callback. Celery runs
`t.sum([10, 20])` — one argument, the list of results.

**chord-in.** Celery builds the chord. Its part runs on the Celery worker first
and writes `.j`; this side's part runs second, sees the count reach the size, and
closes the chord by publishing the callback. The callback still comes out in
index order — `[10, 20]`, not `[20, 10]` — because the join is the sorted set and
not the order things happened to finish.

**recover.** A Celery worker is killed with `kill -9` while holding a 30-second
task. Its reservation stays in Celery's `unacked` hash and index; `recover()` here
reads that table, puts the message back on the queue, and a fresh worker runs it
again — two entries in the log, two pids.

**restore.** The mirror image. This library claims a message and exits without
settling it, leaving the reservation in the same table. A Celery worker started
with a 5-second visibility timeout restores it and runs it, and the table is
empty afterwards.

**eta-out.** A message due in four seconds goes onto the queue, not into a
private sorted set — Celery's client does the same, and a Celery worker has to be
able to see it. The worker takes it immediately and holds it: the log's `at` is
the `due` it was published with, to within 2 ms.

**eta-in.** Celery publishes three seconds out; the message is on the queue
straight away. This library's consumer takes it, parks it in its own delay set
rather than running it early, and runs it when the eta comes.

## Where the two models had to be reconciled

- **The argument tuple.** This library carries one argument value, Celery a tuple
  of them. An array in `args` is the tuple; `Message.single_arg` marks the case
  where the value *is* an array that must stay one — a chord's callback. The flag
  is set when reading a Celery envelope with one positional argument, and when a
  chord's callback is published, so `t.sum([10, 20])` arrives as a list on both
  sides.
- **A chain link's arguments.** Celery prepends the previous link's result unless
  the link is `immutable`. `Message.immutable` carries that, the canvas sets it
  on every link it builds, and the worker honours it when advancing a chain, so
  either side can advance a chain the other started.
- **The join.** Celery's redis backend keeps a chord's parts in `.j` (a sorted
  set scored by index), the size in `.s` and an adjustment in `.t`. `settle` on
  the redis counter writes exactly those, which is why the count can be raised by
  a part run here and closed by a part run there.
- **Reservations.** Celery keeps one `unacked` hash for the whole server plus an
  index scored by claim time, and restores by visibility timeout.
  `Broker::celery` writes that table instead of per-consumer lists, so both
  restorers see the same claims.
- **Priority.** Celery splits a queue across priority steps (`celery`,
  `celery\x06\x163`, …). Publishing routes by priority and consuming reads every
  step, so a message published with a priority on either side is not left behind.

## What is not covered

- Celery's result features that run through its own callbacks — `link`,
  `link_error`, honouring `ignore_result` and `expires` on its side — are
  Celery's own paths and are not exercised here.
- A Celery worker restores an expired claim on its own schedule (once every ten
  polls), which can be minutes. The `restore` scenario starts the worker after
  the claim has aged; a long-running worker would still get there, later.
- A chord closed by two parts that settle in the same instant may publish its
  callback twice: the count is raised and read as two commands, which is how
  Celery's own join behaves as well.
- `run_links.sh` is not part of CI: it needs a venv with Celery. The MoonBit side
  of it — `moontask-redis/src/tests/celery_layout_test.mbt` — is.
- The retry differential (`retry_ladder.py`, `compare_ladder.py`,
  `*-ladder.jsonl`) is the earlier comparison of the same ladder on both sides;
  it is unrelated to the links above and still valid.
