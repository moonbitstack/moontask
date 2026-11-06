// The beat: what it sends, when, and what it does with a schedule that has
// already fired for the last time.

///|
async test "an interval schedule fires once per interval" {
  let manual = Manual::new(1_700_000_000_000L)
  let (app, _) = app_on(fn() { manual.now() })
  let sent = []
  let tick = app.task("tick", fn(_ : Unit, _) -> Unit { sent.push(1) })
  let beat = app.beat()
  beat.add("tick", tick, (), @moontask.Schedule::Every(interval=1000L))
  let worker = app.worker()
  @async.with_task_group(async fn(group) {
    group.spawn_bg(async fn() { worker.run() }, no_wait=true)
    group.spawn_bg(async fn() { beat.run() }, no_wait=true)
    settle(20)
    assert_eq(sent.length(), 0)
    manual.advance(1000L)
    settle(70)
    assert_eq(sent.length(), 1)
    manual.advance(1000L)
    settle(70)
    assert_eq(sent.length(), 2)
    beat.stop()
    worker.stop()
  })
}

///|
async test "an instant schedule fires once and then stops being pending" {
  let manual = Manual::new(1_700_000_000_000L)
  let (app, _) = app_on(fn() { manual.now() })
  let sent = []
  let once = app.task("once", fn(_ : Unit, _) -> Unit { sent.push(1) })
  let beat = app.beat()
  beat.add("once", once, (), @moontask.Schedule::At(at=1_700_000_005_000L))
  let worker = app.worker()
  @async.with_task_group(async fn(group) {
    group.spawn_bg(async fn() { worker.run() }, no_wait=true)
    group.spawn_bg(async fn() { beat.run() }, no_wait=true)
    settle(20)
    assert_eq(sent.length(), 0)
    manual.advance(5000L)
    settle(70)
    assert_eq(sent.length(), 1)
    manual.advance(60000L)
    settle(70)
    assert_eq(sent.length(), 1)
    assert_eq(beat.pending().length(), 0)
    beat.stop()
    worker.stop()
  })
}

///|
async test "a cron schedule fires on the boundary it names" {
  let manual = Manual::new(1_700_000_000_000L)
  let (app, _) = app_on(fn() { manual.now() })
  let sent = []
  let cron = app.task("cron", fn(_ : Unit, _) -> Unit { sent.push(1) })
  let beat = app.beat()
  let expr = @cron.parse("*/10 * * * * *") catch {
    _ => fail("the expression is valid")
  }
  beat.add("cron", cron, (), @moontask.Schedule::Cron(expr~))
  let worker = app.worker()
  @async.with_task_group(async fn(group) {
    group.spawn_bg(async fn() { worker.run() }, no_wait=true)
    group.spawn_bg(async fn() { beat.run() }, no_wait=true)
    // 1_700_000_000_000 is 22:13:20 UTC, so the next ten-second boundary is at
    // 22:13:30, eleven seconds later.
    settle(20)
    assert_eq(sent.length(), 0)
    manual.advance(9999L)
    settle(70)
    assert_eq(sent.length(), 0)
    manual.advance(1L)
    settle(70)
    assert_eq(sent.length(), 1)
    beat.stop()
    worker.stop()
  })
}

///|
async test "pending lists the entries soonest first" {
  let manual = Manual::new(1_700_000_000_000L)
  let (app, _) = app_on(fn() { manual.now() })
  let tick = app.task("tick", fn(_ : Unit, _) -> Unit {  })
  let beat = app.beat()
  beat.add("late", tick, (), @moontask.Schedule::Every(interval=60_000L))
  beat.add("soon", tick, (), @moontask.Schedule::Every(interval=1000L))
  let pending = beat.pending()
  assert_eq(pending.length(), 2)
  assert_eq(pending[0].0, "soon")
  assert_eq(pending[1].0, "late")
}
