<div align="center">

# moontask

**Work that outlives the request that asked for it.**

Task queues, retries, periodic beats, workflows, and the canvas that chains, groups and joins runs — for MoonBit.

[Home](https://moonbitstack.github.io/moontask/) · [Guide](https://moonbitstack.github.io/moontask/guide.html) · [API](https://moonbitstack.github.io/moontask/api.html) · [mooncakes](https://mooncakes.io/docs/moonbitstack/moontask)

</div>

---

## Declaring a task

`#task.def` on a function declares a task, as Celery's `@app.task` does:

```moonbit nocheck
#task.def(name="orders.place", queue="orders", retries="1")
pub fn place(order : Order, ctx : @moontask.Context) -> String raise {
  ...
}
```

The generator writes a `Tasks` record with a typed handle for each declared task, and
`register_tasks`, which registers them all on an app. A worker calls it to run the tasks, a
producer to send them:

<!-- nocheck -->
```moonbit
let tasks = register_tasks(app)
let receipt = tasks.place.send({ id: "a1", cups: 2 }).get(timeout=5000L)
```

A task takes one argument, the one JSON value its message carries (a record when there are
several), and may end with the run's `Context`; it can be a plain function or an `async fn`. The
options are `name` (the function's name when left out), `queue`, `retries`, `timeout` (`250ms`,
`30s`, `5m` or `1h`), `acks_late` and `ignore_result`, laid over the app's preset; the rest of a
policy is set by calling `App::task` directly, as below.

The generator wires in as a dev_build rule, in `moon.mod`:

<!-- nocheck -->
```
rule(name: "tasks", command: "moonx moonbitstack/moontask/cmd/tasks -C \"$mod_dir\" \"$input\" -o \"$output\"")
```

and in the `moon.pkg` of the package, which imports `moonbitstack/moontask` and
`moonbitlang/core/json` (for the `FromJson` and `ToJson` the argument derives):

<!-- nocheck -->
```
dev_build(rule: "tasks", input: "jobs.mbt", output: "jobs.g.mbt")
formatter(ignore: [ "jobs.g.mbt" ])
```

`moon run src/examples/06-annotated` shows it.

## Without the annotation

An app owns a broker, a result store and a clock. A task is a name, a policy and a body; sending one writes a message and hands back a handle that decodes the stored result back into the body's return type. A worker takes messages off the broker, runs them, and settles each one the way its policy says.

```moonbit
async fn first_run() -> Unit {
  let now = fn() { @env.now().reinterpret_as_int64() }
  let broker = @moontask.MemoryBroker::new(clock=now)
  let app = @moontask.App::new(broker, backend=broker, counter=broker, clock=now)

  let greet = app.task("greet", fn(name : String, ctx : @moontask.Context) {
    "hello \{name}, attempt \{ctx.attempt}"
  })

  let worker = app.worker(concurrency=4)
  @async.with_task_group(async fn(group) {
    group.spawn_bg(async fn() { worker.run() }, no_wait=true)
    let handle = greet.send("ada")
    println(handle.get(timeout=5000L))
    worker.stop()
  })
}
```

## What is in the module

| Package | What it is |
|:--|:--|
| `moontask/spec` | The contracts — `Broker`, `Backend`, `Counter`, `Inspector` — and the message, outcome and state they carry |
| `moontask` | `App`, `Task`, `Handle`, `Worker`, `Beat`, the canvas, the executor a graph runs on, and an in-memory broker that is also a store and a counter |
| `moontask/schedule` | A job on a rhythm inside one process, with no broker behind it |
| `moontask/flow` | Workflows: steps that are tasks, ordered by a state graph, each run stamped with the instant it belongs to |
| `moontask/wait` | The one place a wait is written, so the WebAssembly targets are handled once |

Three further modules live beside it, each with its own `moon.mod` so a queue that does not need them does not download them:

| Module | What it is |
|:--|:--|
| `moontask-redis` | A broker and result store on Redis, with per-consumer leases and recovery of a worker that stopped — or on Celery's own key layout, so one queue can be shared with a Celery worker |
| `moontask-sql` | A result store and a workflow run log on SQL, over any `moondb` async driver, so the same code runs on SQLite, MySQL and PostgreSQL |
| `moontask-admin` | A management interface as an ASGI application: a page, reports, sending, revoking, and a live stream of counts |

## A task's policy

| Field | Meaning | Default |
|:--|:--|:--|
| `queue` | Where the message goes | `"default"` |
| `retries` | How many times a failed run is tried again | `0` |
| `backoff` | The wait between attempts | exponential, 1s base, 10min cap, jittered |
| `timeout` | How long one run may take | no limit |
| `acks_late` | Confirm after the run instead of on receipt | `false` |
| `ignore_result` | Keep no result | `false` |
| `rate` | Runs per window, per worker | no limit |

The jitter inside an exponential backoff is derived from the task id and the attempt rather than drawn at random, so the same task waits the same on every machine and a test can assert the delay.

## The canvas

```moonbit
async fn canvas() -> Unit {
  let now = fn() { 1_700_000_000_000L }
  let broker = @moontask.MemoryBroker::new(clock=now)
  let app = @moontask.App::new(broker, backend=broker, counter=broker, clock=now)
  let step = app.task("step", fn(value : Int32, _) { value * 10 })
  let total = app.task("total", fn(values : Array[Int32], _) {
    let mut sum = 0
    for value in values {
      sum = sum + value
    }
    sum
  })
  let chained = @moontask.chain([step.sig(1), step.sig(2), step.sig(3)]).send(app)
  let grouped = @moontask.group([step.sig(4), step.sig(5)]).send(app)
  let joined = @moontask.chord(
    [step.sig(1), step.sig(2), step.sig(3)],
    total.sig([]),
  ).send(app)
  println("\{chained.ids().length()} \{grouped.ids().length()} \{joined.ids().length()}")
}
```

A chain is a message carrying its own tail. A chord gives every part the same counter key and the callback behind it, so whichever worker finishes last sends the callback; a part that fails for good records the callback as failed rather than leaving it pending.

## On a rhythm

```moonbit
fn rhythm() -> Unit {
  let now = fn() { 1_700_000_000_000L }
  let broker = @moontask.MemoryBroker::new(clock=now)
  let app = @moontask.App::new(broker, backend=broker, counter=broker, clock=now)
  let tick = app.task("tick", fn(_ : Unit, _) { () })
  let nightly = @cron.parse("0 3 * * *") catch { _ => abort("valid expression") }
  let beat = app.beat()
  beat.add("tick", tick, (), @moontask.Schedule::Every(interval=1000L))
  beat.add("nightly", tick, (), @moontask.Schedule::Cron(expr=nightly))
  println(beat.pending().length())
}
```

## Workflows

A flow is a set of steps and the dependencies between them. Each step is an ordinary task, so it runs wherever a worker picks it up; the order comes from a state graph, and each run is stamped with the logical instant it belongs to rather than the time it happened to start.

```moonbit
async fn workflow() -> Unit {
  let now = fn() { 1_700_000_000_000L }
  let broker = @moontask.MemoryBroker::new(clock=now)
  let app = @moontask.App::new(broker, backend=broker, counter=broker, clock=now)
  let extract = app.task("extract", fn(_ : Unit, _) -> Json { { "rows": 3 } })
  let load = app.task("load", fn(args : Json, _) -> Json {
    { "written": 3, "from": args }
  })
  let flow = @flow.Flow::new(
    "etl",
    schedule=@moontask.Schedule::Every(interval=3_600_000L),
  )
  flow.step("extract", extract)
  flow.step("load", load, deps=["extract"])
  let worker = app.worker()
  @async.with_task_group(async fn(group) {
    group.spawn_bg(async fn() { worker.run() }, no_wait=true)
    let run = flow.run(app, 1_700_000_000_000L)
    println(run.to_string())
    println(run.output_of("load").unwrap().stringify())
    worker.stop()
  })
}
```

Three things are worth knowing about the arguments. A step that waits for nothing is called with the run's input; a step that waits for others is called with an object of their answers under their ids, so the fields its body reads are the ids it named. And a step names only steps registered before it, which is what makes a flow acyclic by construction.

`backfill(start, end)` runs every instant the flow's schedule names in a window, each as its own run, and one that fails does not stop the ones behind it. `catchup` fills the instants a late start skipped, from the last run the flow knows about — in this process, or in the recorder when there is one. Give a flow a saver with `keep(saver)` and a run interrupted halfway resumes at the step it stopped on.

A run answers `Run`, whose `status` says whether every step settled, and the record keeps the answers even when one did not. `record(recorder)` writes each run as it ends; `moontask-sql` implements the recorder over a table.

## Results and run records on SQL

`moontask-sql` is a result store and a recorder on one connection. The driver is yours, so the same code runs on SQLite, MySQL and PostgreSQL; nothing here opens a socket.

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

`ensure()` creates the two tables if they are not there yet, and is meant to be called once at start-up: a store that ran DDL on every write would put a schema change on the queue's hot path. The tables are named by `Tables`, so two queues can share one schema.

## Across processes

<!-- nocheck -->
```moonbit
let conn = @socket.Connection::connect(@moonredis.ClientOptions::at("127.0.0.1", 6379))
let broker = @redis.Broker::new(conn, clock=now, consumer="worker-7")
let app = @moontask.App::new(broker, backend=broker.store(), counter=broker.store(), clock=now)
```

A queue is a list per name, messages waiting for their eta sit in a sorted set, and what a consumer holds is a list of its own kept alive by a lease. `broker.recover()` hands back what a worker that stopped refreshing its lease was holding.

The same module also speaks Celery's layout, so a queue can be worked from both sides:

<!-- nocheck -->
```moonbit
let broker = @redis.Broker::celery(conn, clock=now, consumer="worker-7")
```

| | |
|:--|:--|
| the queue | the list named after it, split across Celery's priority steps |
| the payload | Celery's envelope: task and id in `headers`, arguments in the body |
| what a consumer holds | Celery's `unacked` hash and its index, so either side can hand back a claim the other left |
| a chain | each message carries the rest of its chain; a link that is not `immutable` is called with the previous link's result prepended |
| a chord | the parts' results land in Celery's join keys as they settle, so the last part — on either side — closes it |
| an eta | the message sits on the queue and the consumer holds it, which is what Celery does |

`interop/` holds the harness that proves it: `bash interop/check.sh` runs eight scenarios against a real Celery 5.5 — a chain and a chord in each direction, a reservation handed back in each direction, and an eta in each direction — and fails unless each side ran what the scenario claims.

## Writing a broker

Four calls. The receipt a claim hands back is opaque to the library:

```moonbit
pub(open) trait Broker {
  async fn publish(Self, queue : String, message : @spec.Message) -> Unit raise @spec.Refused
  async fn consume(Self, queues : ArrayView[String], prefetch~ : Int32) -> @spec.Delivery raise @spec.Refused
  async fn ack(Self, delivery : @spec.Delivery) -> Unit raise @spec.Refused
  async fn reject(Self, delivery : @spec.Delivery, requeue~ : Bool) -> Unit raise @spec.Refused
}
```

A broker that also implements `Backend` and `Counter` is passed to `App::new` three times, which is what the in-memory one and the Redis one do.

## Examples

Six runnable examples ship in `src/examples`: `01-queue` takes a task from send to answer with one retry in between, `02-canvas` runs a chain, a group and a chord, `03-retry` shows the ladder an exponential backoff climbs, `04-flow` runs a two-step workflow on a worker, `05-floor` records a run as the events the home page's figure replays, and `06-annotated` declares its tasks with `#task.def`. All of them pin their clock, so their output is the same on every run.

```
moon run src/examples/01-queue
moon run src/examples/02-canvas
moon run src/examples/03-retry
moon run src/examples/04-flow
moon run src/examples/05-floor
moon run src/examples/06-annotated
```

## License

Apache-2.0.
