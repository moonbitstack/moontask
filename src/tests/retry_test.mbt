// Retries: how many attempts, how long between them, and what the store shows in
// between.

///|
async test "a failing task is retried and the wait is the policy's backoff" {
  let manual = Manual::new(1_000_000L)
  let (app, broker) = app_on(fn() { manual.now() })
  let attempts = []
  let flaky = app.task(
    "flaky",
    fn(_ : Unit, _) -> Unit raise {
      attempts.push(1)
      fail("not yet")
    },
    policy={
      ..@moontask.policy,
      retries: 2,
      backoff: @moontask.Backoff::Fixed(delay=5000L),
    },
  )
  let handle = flaky.send(())
  let worker = app.worker()
  @async.with_task_group(async fn(group) {
    group.spawn_bg(async fn() { worker.run() }, no_wait=true)
    settle(30)
    assert_eq(attempts.length(), 1)
    // The retry waits five seconds and is not due before then.
    settle(30)
    assert_eq(attempts.length(), 1)
    manual.advance(5000L)
    settle(30)
    assert_eq(attempts.length(), 2)
    manual.advance(5000L)
    settle(30)
    assert_eq(attempts.length(), 3)
    match broker.fetch(handle.ids()[0]) {
      Some(record) => {
        assert_eq(record.state, @spec.Failed)
        assert_eq(record.retries, 2)
        assert_true(record.error.unwrap_or("").contains("not yet"))
      }
      None => fail("nothing was recorded")
    }
    worker.stop()
  })
}

///|
async test "a task that succeeds on the second attempt ends succeeded" {
  let manual = Manual::new(1_000_000L)
  let (app, broker) = app_on(fn() { manual.now() })
  let attempts = []
  let once = app.task(
    "once",
    fn(_ : Unit, _) -> Unit raise {
      if attempts.length() == 0 {
        attempts.push(1)
        fail("first time")
      }
      attempts.push(1)
    },
    policy={
      ..@moontask.policy,
      retries: 1,
      backoff: @moontask.Backoff::Fixed(delay=1000L),
    },
  )
  let handle = once.send(())
  let worker = app.worker()
  @async.with_task_group(async fn(group) {
    group.spawn_bg(async fn() { worker.run() }, no_wait=true)
    settle(30)
    assert_retrying(broker, handle.ids()[0])
    manual.advance(1000L)
    settle(30)
    match broker.fetch(handle.ids()[0]) {
      Some(record) => {
        assert_eq(record.state, @spec.Succeeded)
        assert_eq(record.retries, 1)
      }
      None => fail("nothing was recorded")
    }
    worker.stop()
  })
}

///|
/// Check that the run is queued again, which is the state a caller sees between
/// attempts.
async fn assert_retrying(broker : @moontask.MemoryBroker, id : String) -> Unit {
  match broker.fetch(id) {
    Some(record) => {
      assert_eq(record.state, @spec.Retrying)
      assert_eq(record.retries, 1)
    }
    None => fail("nothing was recorded")
  }
}

///|
async test "the retry budget is the number of retries, not of attempts" {
  let manual = Manual::new(1_000_000L)
  let (app, _) = app_on(fn() { manual.now() })
  let attempts = []
  let always = app.task(
    "always",
    fn(_ : Unit, _) -> Unit raise {
      attempts.push(1)
      fail("again")
    },
    policy={ ..@moontask.policy, retries: 0, },
  )
  let _ = always.send(())
  with_worker(app.worker(), async fn() {
    settle(30)
    assert_eq(attempts.length(), 1)
    settle(30)
    assert_eq(attempts.length(), 1)
  })
}

///|
async test "a run that never finishes is cut at the policy's timeout" {
  let manual = Manual::new(1_000_000L)
  let (app, broker) = app_on(fn() { manual.now() })
  let slow = app.task("slow", async fn(_ : Unit, _) { hang() }, policy={
    ..@moontask.policy,
    timeout: Some(20L),
  })
  let handle = slow.send(())
  with_worker(app.worker(), async fn() {
    let refused = try {
      let _ = handle.get(timeout=5000L)
      ""
    } catch {
      @spec.Refused::Failed(got~, ..) => got
      other => other.to_string()
    }
    assert_true(refused.contains("timed out"))
    match broker.fetch(handle.ids()[0]) {
      Some(record) => assert_eq(record.state, @spec.Failed)
      None => fail("nothing was recorded")
    }
  })
}

///|
async test "ignore_result keeps nothing in the store" {
  let manual = Manual::new(1_000_000L)
  let (app, broker) = app_on(fn() { manual.now() })
  let quiet = app.task("quiet", fn(_ : Unit, _) -> Unit {  }, policy={
    ..@moontask.policy,
    ignore_result: true,
  })
  let handle = quiet.send(())
  with_worker(app.worker(), async fn() {
    settle(40)
    assert_eq(broker.fetch(handle.ids()[0]), None)
  })
}

///|
async test "a rate holds the second run back until the window turns over" {
  let manual = Manual::new(1_000_000L)
  let (app, _) = app_on(fn() { manual.now() })
  let ran = []
  let limited = app.task("limited", fn(_ : Unit, _) -> Unit { ran.push(1) }, policy={
    ..@moontask.policy,
    rate: Some(@moontask.Rate::{ count: 1, per: 10_000L, }),
  })
  let _ = limited.send(())
  let _ = limited.send(())
  let worker = app.worker()
  @async.with_task_group(async fn(group) {
    group.spawn_bg(async fn() { worker.run() }, no_wait=true)
    settle(30)
    assert_eq(ran.length(), 1)
    manual.advance(10_000L)
    settle(40)
    assert_eq(ran.length(), 2)
    worker.stop()
  })
}
