// A chain, a group and a chord, run by one worker, with the order they happened
// as the output.

///|
/// A clock the example sets.
struct Clock {
  at : Int64
}

///|
/// The trail as one line, in the order the steps ran.
fn join(items : Array[String]) -> String {
  let out = StringBuilder()
  let mut first = true
  for item in items {
    if !first {
      out.write_string(", ")
    }
    out.write_string(item)
    first = false
  }
  out.to_string()
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
  let trail = []
  let step = app.task("step", fn(value : Int32, _) {
    trail.push("step \{value}")
    value * 10
  })
  let total = app.task("total", fn(values : Array[Int32], _) {
    let mut sum = 0
    for value in values {
      sum = sum + value
    }
    sum
  })
  let worker = app.worker(concurrency=2)
  @async.with_task_group(async fn(group) {
    group.spawn_bg(async fn() { worker.run() }, no_wait=true)
    let chained = @moontask.chain([step.sig(1), step.sig(2), step.sig(3)]).send(
      app,
    )
    println("chain   " + chained.get(timeout=5000L).stringify())
    println("order   " + join(trail))
    trail.clear()
    let grouped = @moontask.group([step.sig(4), step.sig(5)]).send(app)
    println("group   " + grouped.get(timeout=5000L).stringify())
    let joined = @moontask.chord(
      [step.sig(1), step.sig(2), step.sig(3)],
      total.sig([]),
    ).send(app)
    println("chord   " + joined.get(timeout=5000L).stringify())
    worker.stop()
  })
}
