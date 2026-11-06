// The canvas: runs that follow one another, runs that go together, and the join
// that waits for all of them.

///|
async test "a chain runs its links in order and answers the last one" {
  let manual = Manual::new(1_000_000L)
  let (app, _) = app_on(fn() { manual.now() })
  let order = []
  let step = app.task("step", fn(value : Int32, _) {
    order.push(value)
    value * 10
  })
  let handle = @moontask.chain([step.sig(1), step.sig(2), step.sig(3)]).send(
    app,
  )
  let value = with_worker(app.worker(), async fn() { handle.get(timeout=5000L) })
  assert_eq(value, Json::number(30.0))
  assert_eq(order, [1, 2, 3])
}

///|
async test "a group runs every part and answers their values in order" {
  let manual = Manual::new(1_000_000L)
  let (app, _) = app_on(fn() { manual.now() })
  let square = app.task("square", fn(value : Int32, _) { value * value })
  let handle = @moontask.group([square.sig(2), square.sig(3), square.sig(4)]).send(
    app,
  )
  let value = with_worker(app.worker(), async fn() { handle.get(timeout=5000L) })
  assert_eq(
    value,
    Json::array([Json::number(4.0), Json::number(9.0), Json::number(16.0)]),
  )
}

///|
async test "a chord sends its callback once, with every part's result" {
  let manual = Manual::new(1_000_000L)
  let (app, broker) = app_on(fn() { manual.now() })
  let square = app.task("square", fn(value : Int32, _) { value * value })
  let seen = []
  let total = app.task("total", fn(values : Array[Int32], _) {
    for value in values {
      seen.push(value)
    }
    let mut sum = 0
    for value in values {
      sum = sum + value
    }
    sum
  })
  let handle = @moontask.chord(
    [square.sig(2), square.sig(3), square.sig(4)],
    total.sig([]),
  ).send(app)
  let value = with_worker(app.worker(), async fn() { handle.get(timeout=5000L) })
  assert_eq(value, Json::number(29.0))
  assert_eq(seen, [4, 9, 16])
  // The counter is dropped once the callback has been sent.
  assert_eq(broker.queues().length(), 1)
}

///|
async test "a chord whose part fails does not send its callback" {
  let manual = Manual::new(1_000_000L)
  let (app, broker) = app_on(fn() { manual.now() })
  let okay = app.task("okay", fn(value : Int32, _) { value })
  let broken = app.task("broken", fn(_ : Int32, _) -> Unit raise {
    fail("part failed")
  })
  let fired = []
  let total = app.task("total", fn(_ : Array[Int32], _) -> Unit {
    fired.push(1)
  })
  let sig = @moontask.chord([okay.sig(1), broken.sig(2)], total.sig([]))
  let handle = sig.send(app)
  let callback_id = handle.ids()[0]
  with_worker(app.worker(), async fn() {
    settle(60)
    assert_eq(fired.length(), 0)
    match broker.fetch(callback_id) {
      Some(record) => {
        assert_eq(record.state, @spec.Failed)
        assert_true(record.error.unwrap_or("").contains("chord part failed"))
      }
      None => fail("the callback was left with no state")
    }
  })
}

///|
async test "a chord waits for every part before it settles the callback" {
  let manual = Manual::new(1_000_000L)
  let (app, broker) = app_on(fn() { manual.now() })
  let quick = app.task("quick", fn(value : Int32, _) { value })
  let slow = app.task("slow", async fn(_ : Int32, _) { hang() })
  let fired = []
  let total = app.task("total", fn(_ : Array[Int32], _) -> Unit {
    fired.push(1)
  })
  let sig = @moontask.chord([quick.sig(1), slow.sig(2)], total.sig([]))
  let handle = sig.send(app)
  let callback_id = handle.ids()[0]
  let worker = app.worker()
  @async.with_task_group(async fn(group) {
    group.spawn_bg(async fn() { worker.run() }, no_wait=true)
    settle(60)
    // One part is done and one is still running, so the joined callback has
    // nothing to report yet — in particular it must not be settled as failed.
    assert_eq(fired.length(), 0)
    assert_eq(broker.fetch(callback_id), None)
    worker.stop(grace=1L)
  })
}

///|
async test "a chain of chains is one chain" {
  let manual = Manual::new(1_000_000L)
  let (app, _) = app_on(fn() { manual.now() })
  let order = []
  let step = app.task("step", fn(value : Int32, _) {
    order.push(value)
    value
  })
  let inner = @moontask.chain([step.sig(2), step.sig(3)])
  let handle = @moontask.chain([step.sig(1), inner, step.sig(4)]).send(app)
  let _ = with_worker(app.worker(), async fn() { handle.get(timeout=5000L) })
  assert_eq(order, [1, 2, 3, 4])
}

///|
async test "a canvas built from nothing is refused" {
  let manual = Manual::new(1_000_000L)
  let (app, _) = app_on(fn() { manual.now() })
  let empty = @moontask.chain([])
  let refused = try {
    let _ = empty.send(app)
    false
  } catch {
    @spec.Refused::Empty(_) => true
    _ => false
  }
  assert_true(refused)
}
