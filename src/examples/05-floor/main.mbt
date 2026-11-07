// A recorded run of the queue, printed as the events the homepage figure
// animates.
//
// The figure must not draw a story of its own: what a card does on the page has
// to be what the library did here. So this example drives a fixed scenario on a
// clock it moves by hand, waits for each state the store reports, and prints one
// JSON object per event on stdout. `scripts/gen_floor.py` turns that into
// `docs/assets/floor.json`, and the page replays it.
//
// Nothing here reads the wall clock, so the output is the same on every run.

///|
fn event(
  sb : StringBuilder,
  at : Int64,
  kind : String,
  id : String,
  name : String,
) -> Unit {
  sb.write_string("{\"at\":")
  sb.write_string(at.to_string())
  sb.write_string(",\"kind\":\"")
  sb.write_string(kind)
  sb.write_string("\",\"id\":\"")
  sb.write_string(id)
  sb.write_string("\",\"name\":\"")
  sb.write_string(name)
  sb.write_string("\"}")
  sb.write_char('\n')
}

///|
/// Wait until the store reports `want` for `handle`, or give up. A worker runs
/// concurrently, so the example asks rather than sleeps for a guessed time.
async fn[R] settle(
  handle : @moontask.Handle[R],
  want : @spec.State,
) -> @spec.State {
  let mut seen = @spec.Pending
  for i = 0; i < 400; i = i + 1 {
    seen = handle.state()
    if seen == want {
      return seen
    }
    @wait.nap(5)
  }
  seen
}

///|
async fn main {
  let sb = StringBuilder()
  let clock_at = Ref(1_700_000_000_000L)
  let now = fn() { clock_at.val }
  let broker = @moontask.MemoryBroker::new(clock=now)
  let app = @moontask.App::new(
    broker,
    backend=broker,
    counter=broker,
    clock=now,
  )
  let greet = app.task("greet", fn(name : String, _) { "hello " + name })
  let attempts = Ref(0)
  let flaky = app.task(
    "flaky",
    fn(_ : Unit, _) -> Unit raise {
      attempts.val = attempts.val + 1
      if attempts.val < 2 {
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
    // A card that runs straight through.
    let first = greet.send("ada")
    event(sb, now(), "publish", first.ids()[0], "greet")
    let _ = settle(first, @spec.Succeeded)
    event(sb, now(), "success", first.ids()[0], "greet")
    // A card that fails once and comes back on the backoff.
    let second = flaky.send(())
    event(sb, now(), "publish", second.ids()[0], "flaky")
    let _ = settle(second, @spec.Retrying)
    event(sb, now(), "retry", second.ids()[0], "flaky")
    clock_at.val = clock_at.val + 1000L
    let _ = settle(second, @spec.Succeeded)
    event(sb, now(), "success", second.ids()[0], "flaky")
    // Three cards at once: the fan-out a worker with room takes together.
    clock_at.val = clock_at.val + 1000L
    let together = []
    for index = 0; index < 3; index = index + 1 {
      let handle = greet.send("worker-\{index}")
      together.push(handle)
      event(sb, now(), "publish", handle.ids()[0], "greet")
    }
    for handle in together {
      let _ = settle(handle, @spec.Succeeded)
      event(sb, now(), "success", handle.ids()[0], "greet")
    }
    // A card that is cancelled before anyone runs it.
    clock_at.val = clock_at.val + 1000L
    let revoked = greet.send("nobody")
    event(sb, now(), "publish", revoked.ids()[0], "greet")
    revoked.revoke(terminate=true) catch {
      _ => ()
    }
    event(sb, now(), "revoked", revoked.ids()[0], "greet")
    worker.stop()
  })
  println(sb.to_string())
}
