// The whole queue over redis: an app, a worker, and a caller waiting on a handle.

///|
async test "a worker on redis runs a task and the handle answers" {
  guard live(6379) is Some(conn) else { return }
  let prefix = unique("e2e")
  clear(conn, prefix)
  let clock = Manual::new(1_000_000L)
  let broker = @redis.Broker::new(conn, clock=fn() { clock.now() }, prefix~, consumer="e2e")
  let store = broker.store()
  let app = @moontask.App::new(
    broker,
    backend=store,
    counter=store,
    clock=fn() { clock.now() },
  )
  let echo = app.task("echo", fn(value : Int, _) { value + 1 })
  let handle = echo.send(41)
  let worker = app.worker()
  @async.with_task_group(async fn(group) {
    group.spawn_bg(async fn() { worker.run() }, no_wait=true)
    let value = handle.get(timeout=10000L)
    assert_eq(value, 42)
    worker.stop()
  })
  // The result was written to redis, not to memory: a store on a connection of
  // its own reads the same value back.
  let other = connect() catch { _ => fail("a second connection") }
  let alone = @redis.Store::new(other, prefix~)
  match alone.fetch(handle.ids()[0]) {
    Some(record) => assert_eq(record.state, @spec.Succeeded)
    None => fail("the result was not stored on the server")
  }
}

///|
async test "a chord counts its parts on the server" {
  guard live(6379) is Some(conn) else { return }
  let prefix = unique("chorde2e")
  clear(conn, prefix)
  let clock = Manual::new(1_000_000L)
  let broker = @redis.Broker::new(conn, clock=fn() { clock.now() }, prefix~, consumer="chord")
  let store = broker.store()
  let app = @moontask.App::new(
    broker,
    backend=store,
    counter=store,
    clock=fn() { clock.now() },
  )
  let square = app.task("square", fn(value : Int, _) { value * value })
  let total = app.task("total", fn(values : Array[Int], _) {
    let mut sum = 0
    for value in values {
      sum = sum + value
    }
    sum
  })
  let sig = @moontask.chord([square.sig(2), square.sig(3), square.sig(4)], total.sig([]))
  let handle = sig.send(app)
  let worker = app.worker()
  @async.with_task_group(async fn(group) {
    group.spawn_bg(async fn() { worker.run() }, no_wait=true)
    let value = handle.get(timeout=10000L)
    assert_eq(value, Json::number(29.0))
    worker.stop()
  })
}

///|
async test "a task with an eta is held until its time on the server" {
  guard live(6379) is Some(conn) else { return }
  let prefix = unique("etae2e")
  clear(conn, prefix)
  let clock = Manual::new(1_000_000L)
  let broker = @redis.Broker::new(conn, clock=fn() { clock.now() }, prefix~, consumer="eta")
  let store = broker.store()
  let app = @moontask.App::new(
    broker,
    backend=store,
    counter=store,
    clock=fn() { clock.now() },
  )
  let ran = []
  let later = app.task("later", fn(_ : Unit, _) -> Unit { ran.push(1) })
  let _ = later.send((), countdown=60000L)
  let worker = app.worker()
  @async.with_task_group(async fn(group) {
    group.spawn_bg(async fn() { worker.run() }, no_wait=true)
    settle(80)
    assert_eq(ran.length(), 0)
    clock.at = 1_060_000L
    settle(120)
    assert_eq(ran.length(), 1)
    worker.stop()
  })
}
