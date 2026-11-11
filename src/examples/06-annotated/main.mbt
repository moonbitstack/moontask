// Copyright 2026 Leo Cheng
// SPDX-License-Identifier: Apache-2.0

///|
/// A clock the example moves, so the transcript is the same on every run.
struct Clock {
  mut at : Int64
}

///|
async fn main {
  let clock : Clock = { at: 1_700_000_000_000L, }
  let now = fn() { clock.at }
  let broker = @moontask.MemoryBroker::new(clock=now)
  let app = @moontask.App::new(
    broker,
    backend=broker,
    counter=broker,
    clock=now,
  )
  let tasks = register_tasks(app)
  println("registered \{app.task_names().join(", ")}")
  println(
    "queue      \{tasks.place.policy().queue}, retries \{tasks.place.policy().retries}",
  )
  let worker = app.worker(queues=["orders", "default"])
  @async.with_task_group(async fn(group) {
    group.spawn_bg(async fn() { worker.run() }, no_wait=true)
    let small = tasks.place.send({ id: "a1", cups: 2, })
    println("answer     \{small.get(timeout=5000L)}")
    let large = tasks.place.send({ id: "b2", cups: 12, })
    @wait.nap(20)
    clock.at = clock.at + 60_000L
    println("answer     \{large.get(timeout=5000L)}")
    ignore(tasks.tick.send(()).get(timeout=5000L))
    println("tick       done")
    worker.stop()
  })
}
