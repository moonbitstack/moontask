// Two steps of a workflow, run on a worker, with the dependency between them
// doing the ordering and the run stamped with the instant it belongs to.

///|
async fn main {
  let now = fn() { 1_700_000_000_000L }
  let broker = @moontask.MemoryBroker::new(clock=now)
  let app = @moontask.App::new(
    broker,
    backend=broker,
    counter=broker,
    clock=now,
  )
  let extract = app.task("extract", fn(_ : Unit, _) -> Json { { "rows": 3 } })
  let load = app.task("load", fn(args : Json, _) -> Json {
    let rows = match args {
      Object(fields) =>
        match fields.get("extract") {
          Some(row) => row.stringify()
          None => "none"
        }
      _ => "none"
    }
    { "written": 3, "read": rows }
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
    println("run      \{run.thread}")
    println("status   \{run.status.code()}")
    println("extract  \{run.output_of("extract").unwrap().stringify()}")
    println("load     \{run.output_of("load").unwrap().stringify()}")
    worker.stop()
  })
}
