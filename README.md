<div align="center">

# moontask

**Work that outlives the request that asked for it.**

Task queues, retries, periodic beats, and the canvas that chains, groups and joins runs — for MoonBit.

[Home](https://moonbitstack.github.io/moontask/) · [Guide](https://moonbitstack.github.io/moontask/guide.html) · [API](https://moonbitstack.github.io/moontask/api.html) · [mooncakes](https://mooncakes.io/docs/moonbitstack/moontask)

</div>

---

An app owns a broker, a result store and a clock. A task is a name, a policy and a body; sending one writes a message and hands back a handle that decodes the stored result back into the body's return type. A worker takes messages off the broker, runs them, and settles each one the way its policy says.

```moonbit
let now = fn() { @env.now() }
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
```

## What is in the module

| Package | What it is |
|:--|:--|
| `moontask/spec` | The contracts — `Broker`, `Backend`, `Counter`, `Inspector` — and the message, outcome and state they carry |
| `moontask` | `App`, `Task`, `Handle`, `Worker`, `Beat`, the canvas, and an in-memory broker that is also a store and a counter |
| `moontask/schedule` | A job on a rhythm inside one process, with no broker behind it |
| `moontask/wait` | The one place a wait is written, so the WebAssembly targets are handled once |

Two further modules live beside it, each with its own `moon.mod` so a queue that does not need them does not download them:

| Module | What it is |
|:--|:--|
| `moontask-redis` | A broker and result store on Redis, with per-consumer leases and recovery of a worker that stopped — or on Celery's own key layout, so one queue can be shared with a Celery worker |
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
let chained = @moontask.chain([step.sig(1), step.sig(2), step.sig(3)]).send(app)  // answers the last
let grouped = @moontask.group([step.sig(4), step.sig(5)]).send(app)               // answers a list
let joined  = @moontask.chord([step.sig(1), step.sig(2), step.sig(3)], total.sig([])).send(app)
```

A chain is a message carrying its own tail. A chord gives every part the same counter key and the callback behind it, so whichever worker finishes last sends the callback; a part that fails for good records the callback as failed rather than leaving it pending.

## On a rhythm

```moonbit
let beat = app.beat()
beat.add("tick", tick, (), @moontask.Schedule::Every(interval=1000L))
beat.add("nightly", report, (), @moontask.Schedule::Cron(expr~))
```

## Across processes

```moonbit
let conn = @socket.Connection::connect(@moonredis.ClientOptions::at("127.0.0.1", 6379))
let broker = @redis.Broker::new(conn, clock=now, consumer="worker-7")
let app = @moontask.App::new(broker, backend=broker.store(), counter=broker.store(), clock=now)
```

A queue is a list per name, messages waiting for their eta sit in a sorted set, and what a consumer holds is a list of its own kept alive by a lease. `broker.recover()` hands back what a worker that stopped refreshing its lease was holding.

The same module also speaks Celery's layout, so a queue can be worked from both sides:

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

`interop/` holds the harness that proves it: `bash interop/run_links.sh all` runs eight scenarios against a real Celery 5.5 — a chain and a chord in each direction, a reservation handed back in each direction, and an eta in each direction — and prints what each side ran.

## Writing a broker

Four calls. The receipt a claim hands back is opaque to the library:

```moonbit
pub(open) trait Broker {
  async fn publish(Self, queue : String, message : Message) -> Unit raise Refused
  async fn consume(Self, queues : ArrayView[String], prefetch~ : Int32) -> Delivery raise Refused
  async fn ack(Self, delivery : Delivery) -> Unit raise Refused
  async fn reject(Self, delivery : Delivery, requeue~ : Bool) -> Unit raise Refused
}
```

## Examples

Two runnable examples ship in `src/examples`: `01-queue` takes a task from send to answer with one retry in between, and `02-canvas` runs a chain, a group and a chord. Both pin their clock, so their output is the same on every run.

```
moon run src/examples/01-queue
moon run src/examples/02-canvas
```

## License

Apache-2.0.
