// The fan-out: one run per item, one run per tuple, and one run per slice of
// tuples — each answered as the list of what its runs returned.

///|
/// A clock the example sets.
struct Clock {
  at : Int64
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
  let order = []
  let step = app.task("step", fn(value : Int32, _) {
    order.push(value)
    value * 10
  })
  let add = app.task("add", fn(values : Array[Int32], _) {
    let mut sum = 0
    for value in values {
      sum = sum + value
    }
    sum
  })
  // One worker, so the trail below is the order the runs really took.
  let worker = app.worker(concurrency=1)
  @async.with_task_group(async fn(group) {
    group.spawn_bg(async fn() { worker.run() }, no_wait=true)
    let mapped = step.map([1, 2, 3]).send(app)
    println("map     " + mapped.get(timeout=5000L).stringify())
    let starred = add.starmap([[1, 2], [3, 4]]).send(app)
    println("starmap " + starred.get(timeout=5000L).stringify())
    let chunked = step.chunks([[1], [2], [3], [4], [5]], size=2).send(app)
    println("chunks  " + chunked.get(timeout=5000L).stringify())
    println("order   " + order.map(fn(value) { value.to_string() }).join(", "))
    worker.stop()
  })
}
