// A queue end to end, with a clock the example sets so the transcript is the
// same on every run.

///|
/// A clock the example moves.
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
  let greet = app.task("greet", fn(name : String, ctx : @moontask.Context) {
    "hello \{name}, attempt \{ctx.attempt}"
  })
  let attempts = []
  let flaky = app.task(
    "flaky",
    fn(_ : Unit, _) -> Unit raise {
      attempts.push(1)
      if attempts.length() < 2 {
        fail("not yet")
      }
    },
    policy={
      ..@moontask.policy,
      retries: 2,
      backoff: @moontask.Backoff::Fixed(delay=1000L),
    },
  )
  let worker = app.worker(concurrency=2)
  @async.with_task_group(async fn(group) {
    group.spawn_bg(async fn() { worker.run() }, no_wait=true)
    let greeting = greet.send("ada")
    println("queued  greet -> \{greeting.ids()[0]}")
    println("answer  \{greeting.get(timeout=5000L)}")
    let retried = flaky.send(())
    println("queued  flaky -> \{retried.ids()[0]}")
    @wait.nap(20)
    println("state   \{retried.state()}")
    clock.at = clock.at + 1000L
    @wait.nap(20)
    println("state   \{retried.state()}")
    println("attempts \{attempts.length()}")
    worker.stop()
  })
}
