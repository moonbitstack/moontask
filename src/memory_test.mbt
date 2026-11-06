// The in-memory broker, store, counter and inspector, driven directly.

///|
priv struct Clock {
  mut at : Int64
}

///|
fn clock_at(at : Int64) -> Clock {
  { at, }
}

///|
async test "a message published is the message consumed" {
  let clock = clock_at(1_000_000L)
  let broker = @moontask.MemoryBroker::new(clock=fn() { clock.at })
  let message = @spec.Message::new("m-1", "mail.send", Json::string("hello"))
  broker.publish("default", message)
  let delivery = broker.consume(["default"], prefetch=1)
  assert_eq(delivery.message.id, "m-1")
  assert_eq(delivery.queue, "default")
  assert_eq(delivery.message.args, Json::string("hello"))
  broker.ack(delivery)
}

///|
async test "a settled message is not handed out twice" {
  let clock = clock_at(1_000_000L)
  let broker = @moontask.MemoryBroker::new(clock=fn() { clock.at })
  broker.publish(
    "default",
    @spec.Message::new("m-2", "mail.send", Json::null()),
  )
  let first = broker.consume(["default"], prefetch=1)
  broker.ack(first)
  let info = broker.queues()
  assert_eq(info.length(), 1)
  assert_eq(info[0].ready, 0L)
  assert_eq(info[0].unacked, 0L)
}

///|
async test "the highest priority goes first and the oldest first within it" {
  let clock = clock_at(1_000_000L)
  let broker = @moontask.MemoryBroker::new(clock=fn() { clock.at })
  broker.publish("default", {
    ..@spec.Message::new("a", "t", Json::null()),
    priority: 0,
  })
  broker.publish("default", {
    ..@spec.Message::new("b", "t", Json::null()),
    priority: 5,
  })
  broker.publish("default", {
    ..@spec.Message::new("c", "t", Json::null()),
    priority: 0,
  })
  assert_eq(broker.consume(["default"], prefetch=1).message.id, "b")
  assert_eq(broker.consume(["default"], prefetch=1).message.id, "a")
  assert_eq(broker.consume(["default"], prefetch=1).message.id, "c")
}

///|
async test "a message with an eta waits for it" {
  let clock = clock_at(1_000_000L)
  let broker = @moontask.MemoryBroker::new(clock=fn() { clock.at })
  broker.publish("default", {
    ..@spec.Message::new("m-3", "t", Json::null()),
    eta: Some(1_060_000L),
  })
  let info = broker.queues()
  assert_eq(info[0].ready, 0L)
  assert_eq(info[0].delayed, 1L)
  clock.at = 1_060_000L
  assert_eq(broker.consume(["default"], prefetch=1).message.id, "m-3")
}

///|
async test "a rejected message can be put back or dropped" {
  let clock = clock_at(1_000_000L)
  let broker = @moontask.MemoryBroker::new(clock=fn() { clock.at })
  broker.publish("default", @spec.Message::new("m-4", "t", Json::null()))
  let delivery = broker.consume(["default"], prefetch=1)
  broker.reject(delivery, requeue=true)
  // Back at the front of the queue, and settled in neither direction.
  assert_eq(broker.queues()[0].ready, 1L)
  assert_eq(broker.queues()[0].unacked, 0L)
  let delivery = broker.consume(["default"], prefetch=1)
  broker.reject(delivery, requeue=false)
  assert_eq(broker.queues()[0].ready, 0L)
  assert_eq(broker.queues()[0].unacked, 0L)
  // The queue still works after the drop.
  broker.publish("default", @spec.Message::new("m-4b", "t", Json::null()))
  assert_eq(broker.consume(["default"], prefetch=1).message.id, "m-4b")
}

///|
async test "peek shows what would be taken without taking it" {
  let clock = clock_at(1_000_000L)
  let broker = @moontask.MemoryBroker::new(clock=fn() { clock.at })
  broker.publish("default", @spec.Message::new("m-5", "t", Json::null()))
  broker.publish("default", @spec.Message::new("m-6", "t", Json::null()))
  let seen = broker.peek("default", limit=5)
  assert_eq(seen.length(), 2)
  assert_eq(seen[0].id, "m-5")
  assert_eq(broker.queues()[0].ready, 2L)
}

///|
async test "the counter adds and drops" {
  let clock = clock_at(1_000_000L)
  let broker = @moontask.MemoryBroker::new(clock=fn() { clock.at })
  assert_eq(broker.add("chord:1", 3), 3)
  assert_eq(broker.add("chord:1", -1), 2)
  broker.drop("chord:1")
  assert_eq(broker.add("chord:1", -1), -1)
}

///|
async test "the store keeps the last outcome for an id" {
  let clock = clock_at(1_000_000L)
  let broker = @moontask.MemoryBroker::new(clock=fn() { clock.at })
  assert_eq(broker.fetch("x"), None)
  broker.store("x", @spec.outcome_succeeded(Json::number(1.0), 0, 1_000_000L))
  broker.store("x", @spec.outcome_failed("later", 1, 1_000_001L))
  match broker.fetch("x") {
    Some(record) => {
      assert_eq(record.state, @spec.Failed)
      assert_eq(record.error, Some("later"))
    }
    None => fail("the outcome was lost")
  }
}

///|
async test "a handle answers a stored value" {
  let clock = clock_at(1_000_000L)
  let broker = @moontask.MemoryBroker::new(clock=fn() { clock.at })
  let app = @moontask.App::new(broker, backend=broker, counter=broker, clock=fn() {
    clock.at
  })
  broker.store(
    "run-1",
    @spec.outcome_succeeded(Json::number(7.0), 0, 1_000_000L),
  )
  let handle : @moontask.Handle[Int32] = @moontask.Handle::of(
    app,
    ["run-1"],
    false,
  )
  assert_eq(handle.get(), 7)
}

///|
async test "a handle refuses a failed run with what the body raised" {
  let clock = clock_at(1_000_000L)
  let broker = @moontask.MemoryBroker::new(clock=fn() { clock.at })
  let app = @moontask.App::new(broker, backend=broker, counter=broker, clock=fn() {
    clock.at
  })
  broker.store("run-2", @spec.outcome_failed("kaboom", 2, 1_000_000L))
  let handle : @moontask.Handle[Int32] = @moontask.Handle::of(
    app,
    ["run-2"],
    false,
  )
  let refused = try {
    let _ = handle.get()
    ""
  } catch {
    @spec.Refused::Failed(got~, retries~) => "\{got}/\{retries}"
    other => other.to_string()
  }
  assert_eq(refused, "kaboom/2")
}

///|
async test "a handle reports the state a store holds" {
  let clock = clock_at(1_000_000L)
  let broker = @moontask.MemoryBroker::new(clock=fn() { clock.at })
  let app = @moontask.App::new(broker, backend=broker, counter=broker, clock=fn() {
    clock.at
  })
  let handle : @moontask.Handle[Int32] = @moontask.Handle::of(
    app,
    ["run-3"],
    false,
  )
  assert_eq(handle.state(), @spec.Pending)
  broker.store(
    "run-3",
    @spec.outcome_succeeded(Json::number(1.0), 0, 1_000_000L),
  )
  assert_eq(handle.state(), @spec.Succeeded)
}

///|
async test "an app without a store refuses to wait on a result" {
  let clock = clock_at(1_000_000L)
  let broker = @moontask.MemoryBroker::new(clock=fn() { clock.at })
  let app = @moontask.App::new(broker, clock=fn() { clock.at })
  let handle : @moontask.Handle[Int32] = @moontask.Handle::of(
    app,
    ["run-4"],
    false,
  )
  let refused = try {
    let _ = handle.get()
    false
  } catch {
    @spec.Refused::Missing(_) => true
    _ => false
  }
  assert_true(refused)
}
