// A task that fails twice and then succeeds, with the time of each attempt
// written to stdout as one JSON object per line. It is the same ladder
// `interop/retry_ladder.py` runs on Celery, so the two transcripts can be put
// side by side: same attempts, same gaps.
//
//   moon run src/examples/03-retry

///|
fn clock_ms() -> Int64 {
  @env.now().reinterpret_as_int64()
}

///|
async fn main {
  let broker = @moontask.MemoryBroker::new(clock=clock_ms)
  let app = @moontask.App::new(broker, backend=broker, counter=broker, clock=clock_ms)
  let seen = []
  let flaky = app.task(
    "t.flaky",
    fn(_ : Unit, ctx : @moontask.Context) -> Unit raise {
      seen.push(clock_ms())
      if ctx.attempt < 3 {
        fail("boom")
      }
    },
    policy={
      ..@moontask.policy,
      retries: 2,
      // 250 ms, doubling: the wait before the second attempt is 500 ms and
      // before the third 1000 ms, which is what the Celery ladder asks for.
      backoff: @moontask.Backoff::Exponential(base=250L, cap=10000L, jitter=false),
    },
  )
  let worker = app.worker(concurrency=1)
  @async.with_task_group(async fn(group) {
    group.spawn_bg(async fn() { worker.run() }, no_wait=true)
    let handle = flaky.send(())
    let deadline = clock_ms() + 8000L
    while seen.length() < 3 && clock_ms() < deadline {
      @wait.nap(10)
    }
    let state = handle.state()
    worker.stop()
    for i, at in seen {
      println(
        Json::object({
          "side": Json::string("moontask"),
          "attempt": Json::number((i + 1).to_double()),
          "at": Json::number(at.to_double()),
        }).stringify(),
      )
    }
    println(
      Json::object({
        "side": Json::string("moontask"),
        "state": Json::string(state.code()),
        "attempts": Json::number(seen.length().to_double()),
      }).stringify(),
    )
  })
}
