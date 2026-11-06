// The worker's job: take a message, run it, settle it, record what happened.

///|
async test "a queued task runs and the handle answers its value" {
  let manual = Manual::new(1_000_000L)
  let (app, _) = app_on(fn() { manual.now() })
  let echo = app.task("echo", fn(value : Int32, _) { value + 1 })
  let handle = echo.send(41)
  let value = with_worker(app.worker(), async fn() { handle.get(timeout=5000L) })
  assert_eq(value, 42)
}

///|
async test "the result is recorded against the id the caller was given" {
  let manual = Manual::new(1_000_000L)
  let (app, broker) = app_on(fn() { manual.now() })
  let echo = app.task("echo", fn(value : Int32, _) { value * 2 })
  let handle = echo.send(21)
  with_worker(app.worker(), async fn() {
    let _ = handle.get(timeout=5000L)
    let outcome = broker.fetch(handle.ids()[0])
    match outcome {
      Some(record) => {
        assert_eq(record.state, @spec.Succeeded)
        assert_eq(record.result, Some(Json::number(42.0)))
      }
      None => fail("nothing was recorded")
    }
  })
}

///|
async test "a task that raises is recorded as failed with its message" {
  let manual = Manual::new(1_000_000L)
  let (app, broker) = app_on(fn() { manual.now() })
  let broken = app.task("broken", fn(_ : Int32, _) -> Unit raise {
    fail("boom")
  })
  let handle = broken.send(1)
  with_worker(app.worker(), async fn() {
    let refused = try {
      let _ = handle.get(timeout=5000L)
      ""
    } catch {
      @spec.Refused::Failed(got~, ..) => got
      other => other.to_string()
    }
    assert_true(refused.contains("boom"))
    match broker.fetch(handle.ids()[0]) {
      Some(record) => assert_eq(record.state, @spec.Failed)
      None => fail("nothing was recorded")
    }
  })
}

///|
async test "a message for a name nobody registered is rejected, not run" {
  let manual = Manual::new(1_000_000L)
  let (app, broker) = app_on(fn() { manual.now() })
  let orphan : @spec.Message = {
    ..@spec.Message::new("orphan-1", "nobody.registered.this", Json::null()),
  }
  broker.publish("default", orphan)
  with_worker(app.worker(), async fn() {
    settle(50)
    match broker.fetch("orphan-1") {
      Some(record) => assert_eq(record.state, @spec.Failed)
      None => fail("nothing was recorded")
    }
  })
}

///|
async test "a worker stops only after the run in flight has finished" {
  let manual = Manual::new(1_000_000L)
  let (app, _) = app_on(fn() { manual.now() })
  let done = []
  let slow = app.task("slow", async fn(_ : Int32, _) {
    settle(40)
    done.push(1)
  })
  let worker = app.worker()
  @async.with_task_group(async fn(group) {
    group.spawn_bg(async fn() { worker.run() }, no_wait=true)
    let _ = slow.send(1)
    settle(20)
    worker.stop(grace=30000L)
  })
  assert_eq(done.length(), 1)
}

///|
async test "a stop that runs out of grace cancels the run in flight" {
  let manual = Manual::new(1_000_000L)
  let (app, _) = app_on(fn() { manual.now() })
  let done = []
  let slow = app.task("slow", async fn(_ : Int32, _) {
    hang()
    done.push(1)
  })
  let worker = app.worker()
  @async.with_task_group(async fn(group) {
    group.spawn_bg(async fn() { worker.run() }, no_wait=true)
    let _ = slow.send(1)
    settle(20)
    worker.stop(grace=1L)
  })
  assert_eq(done.length(), 0)
}

///|
async test "a task waiting on its eta is not run early" {
  let manual = Manual::new(1_000_000L)
  let (app, _) = app_on(fn() { manual.now() })
  let ran = []
  let later = app.task("later", fn(_ : Unit, _) { ran.push(1) })
  let _ = later.send((), countdown=60000L)
  let worker = app.worker()
  @async.with_task_group(async fn(group) {
    group.spawn_bg(async fn() { worker.run() }, no_wait=true)
    settle(50)
    assert_eq(ran.length(), 0)
    manual.advance(60000L)
    settle(50)
    assert_eq(ran.length(), 1)
    worker.stop()
  })
}

///|
async test "a task past its expires is revoked instead of run" {
  let manual = Manual::new(1_000_000L)
  let (app, broker) = app_on(fn() { manual.now() })
  let ran = []
  let stale = app.task("stale", fn(_ : Unit, _) { ran.push(1) })
  let handle = stale.send((), expires=900_000L)
  with_worker(app.worker(), async fn() {
    settle(50)
    assert_eq(ran.length(), 0)
    match broker.fetch(handle.ids()[0]) {
      Some(record) => assert_eq(record.state, @spec.Revoked)
      None => fail("nothing was recorded")
    }
  })
}

///|
async test "a task revoked before it is taken does not run" {
  let manual = Manual::new(1_000_000L)
  let (app, _) = app_on(fn() { manual.now() })
  let ran = []
  let late = app.task("late", fn(_ : Unit, _) { ran.push(1) })
  let handle = late.send((), countdown=1000L)
  handle.revoke()
  manual.advance(2000L)
  with_worker(app.worker(), async fn() {
    settle(50)
    assert_eq(ran.length(), 0)
  })
}

///|
async test "revoke with terminate stops a run that is already going" {
  let manual = Manual::new(1_000_000L)
  let (app, _) = app_on(fn() { manual.now() })
  let finished = []
  let slow = app.task("slow", async fn(_ : Unit, _) {
    hang()
    finished.push(1)
  })
  let handle = slow.send(())
  let worker = app.worker()
  @async.with_task_group(async fn(group) {
    group.spawn_bg(async fn() { worker.run() }, no_wait=true)
    settle(20)
    handle.revoke(terminate=true)
    settle(50)
    assert_eq(finished.length(), 0)
    worker.stop(grace=1L)
  })
}

///|
async test "prefetch is what the worker asks its broker for" {
  let manual = Manual::new(1_000_000L)
  let (app, _) = app_on(fn() { manual.now() })
  let echo = app.task("echo", fn(value : Int32, _) { value })
  let handle = echo.send(7)
  let worker = app.worker(prefetch=3)
  assert_eq(worker.prefetch(), 3)
  let value = with_worker(worker, async fn() { handle.get(timeout=5000L) })
  assert_eq(value, 7)
}
