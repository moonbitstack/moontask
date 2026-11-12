---
name: moontask
description: Task queues, retries, periodic beats, workflows and the chain/group/chord/map canvas for MoonBit. Use when work must outlive the request that asked for it, when a job has to run on a rhythm, when steps depend on each other, when one call has to fan out over a list, or when results and workflow history have to be kept in a database.
---

# moontask

Install `moonbitstack/moontask` for the queue, and `moonbitstack/moontask-redis` or `moonbitstack/moontask-sql` when the queue crosses processes or the results must outlive one. Import `moonbitstack/moontask` for the queue itself and `moontask/spec` for the contracts.

## Run a task in the same process

```moonbit
async fn first_run() -> Unit {
  let now = fn() { @env.now().reinterpret_as_int64() }
  let broker = @moontask.MemoryBroker::new(clock=now)
  let app = @moontask.App::new(broker, backend=broker, counter=broker, clock=now)
  let greet = app.task("greet", fn(name : String, _ : @moontask.Context) { "hello " + name })
  let worker = app.worker(concurrency=2)
  @async.with_task_group(async fn(group) {
    group.spawn_bg(async fn() { worker.run() }, no_wait=true)
    let handle = greet.send("ada")
    println(handle.get(timeout=5000L))
    worker.stop()
  })
}
```

`App::new` takes the clock as a function: the library never reads the system clock, which is what makes a test able to move time.

## Declare tasks with `#task.def`

Put `#task.def` on a function and wire `moonx moonbitstack/moontask/cmd/tasks` in as a dev_build rule; the generator writes a `Tasks` record of typed handles and `register_tasks(app)`:

<!-- nocheck -->
```moonbit
#derive(ToJson, FromJson)
pub(all) struct Order {
  id : String
  cups : Int
}

#task.def(name="orders.place", queue="orders", retries="1", timeout="30s")
pub fn place(order : Order, ctx : @moontask.Context) -> String raise {
  "\{order.id} on attempt \{ctx.attempt}"
}

// elsewhere, on the worker and on the producer alike
let tasks = register_tasks(app)
let answer = tasks.place.send({ id: "a1", cups: 2 }).get(timeout=5000L)
```

- One argument per task, the JSON value the message carries; several values go in a record that derives `ToJson` and `FromJson`, so the package imports `moonbitlang/core/json`.
- A trailing `@moontask.Context` parameter is optional; `ctx.attempt` starts at 1.
- Options: `name`, `queue`, `retries`, `timeout` (`250ms`, `30s`, `5m`, `1h`), `acks_late`, `ignore_result`. Anything else in a policy goes through `App::task` directly.

## Retry a task

```moonbit
async fn retrying() -> Unit {
  let now = fn() { 1_700_000_000_000L }
  let broker = @moontask.MemoryBroker::new(clock=now)
  let app = @moontask.App::new(broker, backend=broker, counter=broker, clock=now)
  let attempts = []
  let flaky = app.task("flaky", fn(_ : Unit, _) -> Unit raise {
    attempts.push(1)
    if attempts.length() < 2 {
      fail("not yet")
    }
  }, policy={
    ..@moontask.policy,
    retries: 3,
    backoff: @moontask.Backoff::Exponential(base=1000L, cap=60000L, jitter=true),
    timeout: Some(30000L),
    acks_late: true,
  })
  println(flaky.policy().retries)
}
```

A body that raises is retried inside the budget with an `eta` from the backoff; past it the failure is stored. `Handle::get` raises `Refused::Failed` with the message; `Handle::state` answers `Retrying` between attempts.

## Run steps in order, or join them

```moonbit
async fn canvas() -> Unit {
  let now = fn() { 1_700_000_000_000L }
  let broker = @moontask.MemoryBroker::new(clock=now)
  let app = @moontask.App::new(broker, backend=broker, counter=broker, clock=now)
  let step = app.task("step", fn(value : Int32, _) { value * 10 })
  let total = app.task("total", fn(values : Array[Int32], _) { values.length() })
  let chained = @moontask.chain([step.sig(1), step.sig(2)]).send(app)
  let grouped = @moontask.group([step.sig(3), step.sig(4)]).send(app)
  let joined = @moontask.chord([step.sig(5), step.sig(6)], total.sig([])).send(app)
  println("\{chained.ids().length()} \{grouped.ids().length()} \{joined.ids().length()}")
}
```

A chord needs an app with a `Counter`; the in-memory broker and the Redis store both are one. A part that fails for good records the callback as failed rather than leaving it pending.

## Fan out over a list

```moonbit
async fn fan_out() -> Unit {
  let now = fn() { 1_700_000_000_000L }
  let broker = @moontask.MemoryBroker::new(clock=now)
  let app = @moontask.App::new(broker, backend=broker, counter=broker, clock=now)
  let step = app.task("step", fn(value : Int32, _) { value * 10 })
  let add = app.task("add", fn(values : Array[Int32], _) { values.length() })
  let worker = app.worker()
  @async.with_task_group(async fn(task_group) {
    task_group.spawn_bg(async fn() { worker.run() }, no_wait=true)
    println(step.map([1, 2, 3]).send(app).get(timeout=5000L).stringify())
    println(add.starmap([[1, 2], [3, 4]]).send(app).get(timeout=5000L).stringify())
    println(step.chunks([[1], [2], [3]], size=2).send(app).get(timeout=5000L).stringify())
    worker.stop()
  })
}
```

- `map(items)` publishes one run per item, each item being that run's whole argument; the handle answers the results in order.
- `starmap(tuples)` publishes one run per tuple, the tuple being that run's argument list; a tuple of one travels as that single argument, a longer one as the list. That is the same reading a consumer on the other side of a shared queue gets.
- `chunks(tuples, size=2)` publishes one message per slice; the worker that takes a slice runs its tuples in order and answers their list. Fewer messages and fewer reservations, at the cost of serial work inside one run — and a slice is retried as a whole under the chunk task's default policy, not per tuple.
- An empty fan-out publishes nothing and answers an empty list, while `chain([])` is refused.
- The chunk body is registered under `moontask.chunks` by `App::worker`, so a worker runs a chunk another process built; `chunk_name` is that name if a policy has to be set for it.

## Send on a rhythm

```moonbit
fn rhythm() -> Unit {
  let now = fn() { 1_700_000_000_000L }
  let broker = @moontask.MemoryBroker::new(clock=now)
  let app = @moontask.App::new(broker, backend=broker, counter=broker, clock=now)
  let tick = app.task("tick", fn(_ : Unit, _) { () })
  let expr = @cron.parse("0 3 * * *") catch { _ => abort("valid expression") }
  let beat = app.beat()
  beat.add("tick", tick, (), @moontask.Schedule::Every(interval=1000L))
  beat.add("nightly", tick, (), @moontask.Schedule::Cron(expr~))
  println(beat.pending().length())
}
```

Without a worker behind it, `moonbitstack/moontask/schedule` runs a job inside one process: `Scheduler::new(clock=now)`, `add(every(10).minutes().at(":30").run(action))`, `run()`.

## Depend on other steps: a workflow

```moonbit
async fn workflow() -> Unit {
  let now = fn() { 1_700_000_000_000L }
  let broker = @moontask.MemoryBroker::new(clock=now)
  let app = @moontask.App::new(broker, backend=broker, counter=broker, clock=now)
  let extract = app.task("extract", fn(_ : Unit, _) -> Json { { "rows": 3 } })
  let load = app.task("load", fn(args : Json, _) -> Json { { "from": args } })
  let flow = @flow.Flow::new("etl", schedule=@moontask.Schedule::Every(interval=3_600_000L))
  flow.step("extract", extract)
  flow.step("load", load, deps=["extract"])
  let worker = app.worker()
  @async.with_task_group(async fn(group) {
    group.spawn_bg(async fn() { worker.run() }, no_wait=true)
    let run = flow.run(app, 1_700_000_000_000L)
    println(run.status.code())
    println(run.output_of("load").unwrap().stringify())
    worker.stop()
  })
}
```

- A step is an ordinary task, so it runs wherever a worker picks it up. The order comes from a state graph, not from the process that called `run`.
- A step that waits for nothing is called with the run's input; one that waits for others with an object of their answers under their ids.
- A step may name only steps registered before it, which keeps the flow acyclic by construction.
- `backfill(start, end)` runs every instant of the schedule in the window; `catchup(now)` fills what a late start skipped; `keep(saver)` makes a run resumable at the step it stopped on.
- `run` answers a `Run` even when a step failed: read `status`, and `output_of(id)` for what each step answered.

## Share the queue between processes

<!-- nocheck -->
```moonbit
let conn = @socket.Connection::connect(@moonredis.ClientOptions::at("127.0.0.1", 6379))
let broker = @redis.Broker::new(conn, clock=now, consumer="worker-7")
let app = @moontask.App::new(broker, backend=broker.store(), counter=broker.store(), clock=now)
```

- One connection serves one command at a time; share it through `broker.store()` rather than wrapping it twice.
- Give each worker process its own `consumer` name. `broker.recover()` hands back what a worker that stopped refreshing its lease was holding.
- A broker with concurrency above one wants a connection per task, or a pooled `RedisConn`.

## Share the queue with a Celery worker

<!-- nocheck -->
```moonbit
let broker = @redis.Broker::celery(conn, clock=now, consumer="worker-7")
let app = @moontask.App::new(broker, backend=broker.store(), counter=broker.store(), clock=now)
```

The queue is the list named after it, the payload is Celery's envelope, and the reservation table is Celery's own `unacked` hash — so either side can run what the other published, and either side's restore can hand back a claim a dead worker left.

- A message with an `eta` goes on the queue and the consumer holds it until then, which is what Celery does. Do not expect a delayed message to be absent from the queue.
- A chain published here marks every link `immutable`, so each step is called with exactly the arguments it declared. Celery's links are not immutable: its worker prepends the previous link's result. Reading a Celery chain keeps that flag, so a chain may start on one side and continue on the other.
- A chord's part carries its group and index in the headers and the callback in the body; the join is Celery's own `.j`/`.t`/`.s` keys, written by whichever side settles the part. Either side may close the chord.
- `settle` is the call a worker makes; `add` only opens a chord.

## Keep results and workflow history in SQL

<!-- nocheck -->
```moonbit
import {
  "moonbitstack/moontask",
  "moonbitstack/moontask/flow",
  "moonbitstack/moontask-sql/sql",
  "moonbitstack/moonpostgres",
}

async fn sql_backend {
  let conn = @moonpostgres.PgConn::connect("127.0.0.1", 5432, "postgres", "postgres", "test")
  let store = @sql.Store::new(conn, clock=fn() { @env.now().reinterpret_as_int64() })
  store.ensure()
  let app = @moontask.App::new(broker, backend=store, clock=fn() { @env.now().reinterpret_as_int64() })
  let flow = @flow.Flow::new("etl")
  flow.record(store)
}
```

- The driver is the caller's, so the same store runs on SQLite, MySQL and PostgreSQL; the dialect is read from the driver once.
- A driver that answers in the same call — SQLite's — goes in through `Store::inline(db, clock=now)`; a server's asynchronous driver through `Store::new`.
- `ensure()` creates both tables if they are missing; call it once at start-up, not on every write.
- `Store` is both a `Backend` (one row per run) and a `Recorder` (one row per workflow run), so an app and a flow can share it.
- Name the tables with `Tables` when two queues share one schema.

## Things that catch people

- `eta` and `expires` are milliseconds since the epoch; `countdown` is a delay from now and loses to `eta`.
- `acks_late: false` confirms the message before the body runs, so a worker killed mid-run does not repeat the work. Set it true only for work that can be repeated.
- A `Handle` needs an app with a `Backend`; without one `get` refuses with `Refused::Missing`.
- One argument is not the same as one list: `single_arg` marks a message whose single argument is itself an array — a chord's callback, for instance. It is set for you when reading Celery's envelope and when a chord's callback is published.
- `Refused` is the only error type of the queue: a failure of the body arrives as `Refused::Failed`, a broker or store problem as `Refused::Transport`. A flow's definition errors are `@flow.Refused`, because a graph that cannot be built is not a queue that refused.
- Do not run a flow's graph with `@moonstate.inline`: the step bodies live on the workers, and the graph says so instead of writing nothing.
