---
name: moontask
description: Task queues, retries, periodic beats and the chain/group/chord canvas for MoonBit. Use when work must outlive the request that asked for it, when a job has to run on a rhythm, or when several steps must run in order or be joined.
---

# moontask

Install `moonbitstack/moontask` (and `moonbitstack/moontask-redis` if the queue is shared between processes). Import `moonbitstack/moontask` for the queue and `moontask/spec` for the contracts.

## Run a task in the same process

```moonbit
import {
  "moonbitstack/moontask",
  "moonbitlang/async",
}

async fn main {
  let now = fn() { @env.now() }
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

## Retry a task

```moonbit
let flaky = app.task("flaky", body, policy={
  ..@moontask.policy,
  retries: 3,
  backoff: @moontask.Backoff::Exponential(base=1000L, cap=60000L, jitter=true),
  timeout: Some(30000L),
  acks_late: true,
})
```

A body that raises is retried inside the budget with an `eta` from the backoff; past it the failure is stored. `Handle::get` raises `Refused::Failed` with the message; `Handle::state` answers `RETRYING` between attempts.

## Run steps in order, or join them

```moonbit
let chained = @moontask.chain([step.sig(1), step.sig(2)]).send(app)   // answers the last
let grouped = @moontask.group([step.sig(3), step.sig(4)]).send(app)   // answers a list
let joined = @moontask.chord([step.sig(5), step.sig(6)], total.sig([])).send(app)
```

A chord needs an app with a `Counter`; the in-memory broker and the Redis store both are one. A part that fails for good records the callback as failed rather than leaving it pending.

## Send on a rhythm

```moonbit
let beat = app.beat()
beat.add("tick", tick, (), @moontask.Schedule::Every(interval=1000L))
group.spawn_bg(async fn() { beat.run() }, no_wait=true)
```

Without a worker behind it, `moonbitstack/moontask/schedule` runs a job inside one process: `Scheduler::new(clock=now)`, `add(every(10).minutes().at(":30").run(action))`, `run()`.

## Share the queue between processes

```moonbit
let conn = @socket.Connection::connect(@moonredis.ClientOptions::at("127.0.0.1", 6379))
let broker = @redis.Broker::new(conn, clock=now, consumer="worker-7")
let app = @moontask.App::new(broker, backend=broker.store(), counter=broker.store(), clock=now)
```

- One connection serves one command at a time; share it through `broker.store()` rather than wrapping it twice.
- Give each worker process its own `consumer` name. `broker.recover()` hands back what a worker that stopped refreshing its lease was holding.
- A broker with concurrency above one wants a connection per task, or a pooled `RedisConn`.

## Things that catch people

- `eta` and `expires` are milliseconds since the epoch; `countdown` is a delay from now and loses to `eta`.
- `acks_late: false` confirms the message before the body runs, so a worker killed mid-run does not repeat the work. Set it true only for work that can be repeated.
- A `Handle` needs an app with a `Backend`; without one `get` refuses with `Refused::Missing`.
- `Refused` is the only error type: a failure of the body arrives as `Refused::Failed`, a broker or store problem as `Refused::Transport`.
