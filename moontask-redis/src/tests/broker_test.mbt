// The broker's four calls, and the store beside it, against a real server.

///|
async test "a published message comes back in the order it was published" {
  guard live(6379) is Some(conn) else { return }
  let prefix = unique("order")
  clear(conn, prefix)
  let clock = Manual::new(1_000_000L)
  let broker = @redis.Broker::new(
    conn,
    clock=fn() { clock.now() },
    prefix~,
    consumer="a",
  )
  for name in ["m-1", "m-2", "m-3"] {
    broker.publish("default", @spec.Message::new(name, "task", Json::null()))
  }
  let seen = []
  for _ in 0..<3 {
    let delivery = broker.consume(["default"], prefetch=1)
    seen.push(delivery.message.id)
    broker.ack(delivery)
  }
  assert_eq(seen, ["m-1", "m-2", "m-3"])
  assert_eq(broker.queues()[0].ready, 0L)
}

///|
async test "a message waits for its eta in the sorted set" {
  guard live(6379) is Some(conn) else { return }
  let prefix = unique("eta")
  clear(conn, prefix)
  let clock = Manual::new(1_000_000L)
  let broker = @redis.Broker::new(
    conn,
    clock=fn() { clock.now() },
    prefix~,
    consumer="a",
  )
  broker.publish("default", {
    ..@spec.Message::new("m-eta", "task", Json::null()),
    eta: Some(1_000_500L),
  })
  let info = broker.queues()
  assert_eq(info[0].ready, 0L)
  assert_eq(info[0].delayed, 1L)
  clock.at = 1_000_500L
  let delivery = broker.consume(["default"], prefetch=1)
  assert_eq(delivery.message.id, "m-eta")
  broker.ack(delivery)
}

///|
async test "a message rejected with requeue comes back" {
  guard live(6379) is Some(conn) else { return }
  let prefix = unique("requeue")
  clear(conn, prefix)
  let clock = Manual::new(1_000_000L)
  let broker = @redis.Broker::new(
    conn,
    clock=fn() { clock.now() },
    prefix~,
    consumer="a",
  )
  broker.publish("default", @spec.Message::new("m-r", "task", Json::null()))
  let delivery = broker.consume(["default"], prefetch=1)
  broker.reject(delivery, requeue=true)
  let again = broker.consume(["default"], prefetch=1)
  assert_eq(again.message.id, "m-r")
  broker.ack(again)
  let info = broker.queues()
  assert_eq(info[0].ready, 0L)
  assert_eq(info[0].unacked, 0L)
}

///|
async test "a message rejected without requeue is gone" {
  guard live(6379) is Some(conn) else { return }
  let prefix = unique("drop")
  clear(conn, prefix)
  let clock = Manual::new(1_000_000L)
  let broker = @redis.Broker::new(
    conn,
    clock=fn() { clock.now() },
    prefix~,
    consumer="a",
  )
  broker.publish("default", @spec.Message::new("m-d", "task", Json::null()))
  let delivery = broker.consume(["default"], prefetch=1)
  broker.reject(delivery, requeue=false)
  let info = broker.queues()
  assert_eq(info[0].ready, 0L)
  assert_eq(info[0].unacked, 0L)
}

///|
async test "peek shows the next messages without claiming them" {
  guard live(6379) is Some(conn) else { return }
  let prefix = unique("peek")
  clear(conn, prefix)
  let clock = Manual::new(1_000_000L)
  let broker = @redis.Broker::new(
    conn,
    clock=fn() { clock.now() },
    prefix~,
    consumer="a",
  )
  broker.publish(
    "default",
    @spec.Message::new("m-p1", "task", Json::string("one")),
  )
  broker.publish(
    "default",
    @spec.Message::new("m-p2", "task", Json::string("two")),
  )
  let seen = broker.peek("default", limit=5)
  assert_eq(seen.length(), 2)
  assert_eq(seen[0].id, "m-p1")
  assert_eq(seen[0].args, Json::string("one"))
  assert_eq(broker.queues()[0].ready, 2L)
}

///|
async test "a result round-trips through the store" {
  guard live(6379) is Some(conn) else { return }
  let prefix = unique("result")
  clear(conn, prefix)
  let store = @redis.Store::new(conn, prefix~)
  assert_eq(store.fetch("x"), None)
  let outcome = @spec.outcome_succeeded(
    Json::array([Json::number(3.0)]),
    1,
    1_000_000L,
  )
  store.store("x", outcome)
  assert_eq(store.fetch("x"), Some(outcome))
  store.store("x", @spec.outcome_failed("boom", 2, 1_000_001L))
  match store.fetch("x") {
    Some(record) => {
      assert_eq(record.state, @spec.Failed)
      assert_eq(record.error, Some("boom"))
    }
    None => fail("the outcome was lost")
  }
}

///|
async test "the chord counter adds and drops on the server" {
  guard live(6379) is Some(conn) else { return }
  let prefix = unique("chord")
  clear(conn, prefix)
  let store = @redis.Store::new(conn, prefix~)
  assert_eq(store.add("k", 3), 3)
  assert_eq(store.add("k", -1), 2)
  store.drop("k")
  assert_eq(store.add("k", -1), -1)
}

///|
async test "a worker that stops without settling leaves its work for recovery" {
  guard live(6379) is Some(conn) else { return }
  let prefix = unique("lease")
  clear(conn, prefix)
  let clock = Manual::new(1_000_000L)
  let first = @redis.Broker::new(
    conn,
    clock=fn() { clock.now() },
    prefix~,
    consumer="first",
    lease=300L,
  )
  first.publish("default", @spec.Message::new("m-l", "task", Json::null()))
  let delivery = first.consume(["default"], prefetch=1)
  assert_eq(delivery.message.id, "m-l")
  // The consumer is gone: its lease expires and nobody refreshes it.
  @async.sleep(400)
  let second = @redis.Broker::new(
    conn,
    clock=fn() { clock.now() },
    prefix~,
    consumer="second",
  )
  let moved = second.recover()
  assert_true(moved >= 1L)
  let again = second.consume(["default"], prefetch=1)
  assert_eq(again.message.id, "m-l")
  second.ack(again)
}

///|
async test "a live consumer keeps its work out of recovery" {
  guard live(6379) is Some(conn) else { return }
  let prefix = unique("keeplive")
  clear(conn, prefix)
  let clock = Manual::new(1_000_000L)
  let broker = @redis.Broker::new(
    conn,
    clock=fn() { clock.now() },
    prefix~,
    consumer="live",
  )
  broker.publish("default", @spec.Message::new("m-keep", "task", Json::null()))
  let delivery = broker.consume(["default"], prefetch=1)
  // The same consumer runs recovery: its own lease is still there, so its
  // message stays claimed.
  let moved = broker.recover()
  assert_eq(moved, 0L)
  broker.ack(delivery)
}
