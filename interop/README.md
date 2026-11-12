# Interop with Celery 5

The harness behind every claim this library makes about sharing a queue with
Celery. Nothing here is a mock: a real `celery==5.5.3` (kombu 5.5.4) runs as a
worker, a real redis holds the queue, and the MoonBit side is the example that
ships in `moontask-redis`.

```sh
bash setup.sh          # a redis, and a venv with celery[redis]==5.5.3
bash run_links.sh all  # the ten scenarios below
bash check.sh          # the same ten, each checked against what it claims
```

Each scenario starts from an empty server, and prints what each side actually
ran, from one log the tasks write to as they start:

```
{"task": "t.step2", "args": [50], "at": 1793976468287, "pid": 14211}
```

## One run, ten scenarios

```
================ chain-out        a chain published here, run by Celery
{"published":"chain-a-1794493320842"}
{"task": "t.step1", "args": [5], "at": 1794493320848, "pid": 6240}
{"task": "t.step2", "args": [50], "at": 1794493320877, "pid": 6240}

================ chain-in         a chain published by Celery, head run here
celery published chain 4ea07a66-0601-40e0-a383-5b7efa36d096
{"ran":1,"tasks":["t.step1(5)@1794493339453"]}
{"task": "t.step2", "args": [50], "at": 1794493339459, "pid": 6479}

================ chord-out        a chord of parts run here, callback on Celery
{"published":"1794493356784:3:t.sum"}
{"ran":2,"tasks":["t.step1(1)@1794493356799","t.step1(2)@1794493356807"]}
{"task": "t.sum", "args": [[10, 20]], "at": 1794493356814, "pid": 6723}

================ chord-in         a chord split across the two, joined here
celery published chord c919109d-dba1-48d3-be80-514cee0bd699
-- parts in so far: 1
{"ran":1,"tasks":["t.step1(1)@1794493376060"]}
{"task": "t.step1", "args": [2], "at": 1794493375795, "pid": 6977}
{"task": "t.sum", "args": [[10, 20]], "at": 1794493376068, "pid": 6977}

================ fanout-out       a map and a starmap published here, run by Celery
{"published":5}
{"task": "t.step1", "args": [1], "at": 1794493394061, "pid": 7234}
{"task": "t.step1", "args": [2], "at": 1794493394078, "pid": 7234}
{"task": "t.step1", "args": [3], "at": 1794493394087, "pid": 7234}
{"task": "t.add", "args": [1, 2], "at": 1794493394096, "pid": 7234}
{"task": "t.add", "args": [3, 4], "at": 1794493394106, "pid": 7234}

================ fanout-in        a fan-out published by Celery, run here
celery published a map of 2 and a starmap of 2 on fanq as 24d157c6-2bec-4226-b256-b95fdbe309d4
{"ran":4,"tasks":["t.step1(4)@1794493408620","t.step1(5)@1794493408629","t.add([10,20])@1794493408639","t.add([30,40])@1794493408649"]}

================ recover          a Celery worker killed mid-task, recovered here
-- the reservation it left: 1
{"moved":1}
-- on the queue now: 1
{"task": "t.slow", "args": [30], "at": 1794493421428, "pid": 7698}
{"task": "t.slow", "args": [30], "at": 1794493430456, "pid": 7771}

================ restore          a claim made here, handed back by Celery
{"claimed":"moontask-1794493443186:1794493443189:1"}
-- the reservation is in Celery's table: 1
{"task": "t.double", "args": [21], "at": 1794493451174, "pid": 8035}
-- the reservation is gone: 0

================ eta-out          an eta published here, honoured by Celery
{"published":"eta-1794493468035","due":1794493472035}
-- on the queue right after publishing: 1
{"task": "t.step1", "args": [5], "at": 1794493472038, "pid": 8332}

================ eta-in           an eta published by Celery, honoured here
celery published 291402d8-9ad8-4d63-80c8-3a32b97d8ffb due at 1794493489300 (3s out)
-- on the queue right after publishing: 1
{"ran":1,"tasks":["t.step1(5)@1794493489267"]}
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

**fanout-out.** `map` publishes one message per item and `starmap` one per tuple:
five messages here, three of them `t.step1(i)` and two of them `t.add(a, b)`. The
Celery worker runs each as it stands — a single argument as one argument, a
two-element tuple spread into the function's two parameters — which is what makes
a fan-out published here runnable there without a wrapper task.

**fanout-in.** The same shape from the other side: Celery publishes four
signatures — two single-argument runs and two two-argument ones — and this
library's worker runs them, reading a one-element argument list as that one
argument (`t.step1(5)`) and a longer list as the list (`t.add([10, 20])`). That
reading is what `Message.single_arg` records, and it is why the argument lists of
a Celery fan-out survive the crossing in both directions.

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
the `due` it was published with, to within a few milliseconds.

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
- **A fan-out's tuples.** `map` takes one argument per run and `starmap` takes a
  tuple; a tuple of one travels as that one argument and a longer one as the
  argument list, which is the same reading a Celery envelope gets. `chunks`
  batches tuples into one run of the chunk task, which runs them in order in the
  worker that took it.
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
- Celery's `celery.map`, `celery.starmap` and `celery.chunks` are wrapper tasks
  of its own: `task.map(it)` publishes *one* message to `celery.map`, which the
  Celery worker then expands into a group. The expanded group is what travels
  between two independent systems, so `fanout-in` publishes that shape and
  `fanout-out` publishes the same one from here. A `celery.map` message published
  by a Celery client is not run by this library, and `chunks` has no Celery
  counterpart on the wire: its payload names the task and carries the tuples, in
  a form this library defines.
- A Celery worker restores an expired claim on its own schedule (once every ten
  polls), which can be minutes. The `restore` scenario starts the worker after
  the claim has aged; a long-running worker would still get there, later.
- A chord closed by two parts that settle in the same instant may publish its
  callback twice: the count is raised and read as two commands, which is how
  Celery's own join behaves as well.
- The retry differential (`retry_ladder.py`, `compare_ladder.py`,
  `*-ladder.jsonl`) is the earlier comparison of the same ladder on both sides;
  it is unrelated to the links above and still valid.

`check.sh` drives all ten through `run_links.sh`, so every scenario above is what
the `interop` job of the workflow checks on each push: a scenario that prints the
wrong task, the wrong argument or nothing at all fails it.
