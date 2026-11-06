// The Celery layout: the queue Celery reads, the reservation table Celery
// restores from, the priority lists, and the keys a chord joins on.
//
// These run against a real server because the point of each is a key: a message
// on the list named after the queue, a tag in `unacked`, a member of `.j`.

///|
/// A broker in Celery's layout over `conn`, with a clock the test moves.
fn celery_broker(
  conn : @socket.Connection,
  clock : Manual,
  visibility~ : Int64 = 3600000L,
) -> @redis.Broker {
  @redis.Broker::celery(
    conn,
    clock=fn() { clock.now() },
    consumer="celery-test",
    visibility~,
  )
}

///|
/// An app over a Celery-layout broker, with its own clock.
fn celery_app(broker : @redis.Broker, clock : Manual) -> @moontask.App {
  @moontask.App::new(broker, backend=broker.store(), counter=broker.store(), clock=fn() {
    clock.now()
  })
}

///|
async test "a message with an eta waits on the queue Celery reads" {
  guard live(6379) is Some(conn) else { return }
  let clock = Manual::new(1_700_000_000_000L)
  let queue = unique("eta")
  clear(conn, queue)
  let broker = celery_broker(conn, clock)
  let probe = @moonredis.Client::new(connect() catch { _ => fail("a probe") })
  let later = {
    ..@spec.Message::new("eta-1", "t.step", Json::number(1)),
    queue,
    eta: Some(clock.now() + 60000L),
  }
  broker.publish(queue, later)
  // Celery's client puts an eta message on the queue and lets the worker hold
  // it, so the list carries it: a Celery worker must be able to see what we
  // send, eta and all.
  assert_eq(probe.llen(queue) catch { _ => -1L }, 1L)

  // Our own consumer must not run it early: taken off the queue before its eta,
  // it is parked in the delay set and published again when it is due.
  let app = celery_app(broker, clock)
  let ran = []
  let _ = app.task("t.step", fn(_ : Int32, _) -> Unit { ran.push(1) })
  let worker = app.worker(queues=[queue], concurrency=1)
  @async.with_task_group(async fn(group) {
    group.spawn_bg(async fn() { worker.run() }, no_wait=true)
    settle(120)
    assert_eq(ran.length(), 0)
    assert_eq(probe.llen(queue) catch { _ => -1L }, 0L)
    assert_eq(probe.zcard("moontask:d:\{queue}") catch { _ => -1L }, 1L)
    clock.at = clock.at + 60000L
    settle(200)
    assert_eq(ran.length(), 1)
    worker.stop(grace=100L)
  })
  ignore(probe.del([queue, "moontask:d:\{queue}"]) catch { _ => 0L })
}

///|
async test "a claim is written where Celery keeps its reservations" {
  guard live(6379) is Some(conn) else { return }
  let clock = Manual::new(1_700_000_000_000L)
  let queue = unique("claim")
  clear(conn, queue)
  let broker = celery_broker(conn, clock)
  let probe = @moonredis.Client::new(connect() catch { _ => fail("a probe") })
  broker.publish(queue, {
    ..@spec.Message::new("claim-1", "t.step", Json::number(1)),
    queue,
  })
  let delivery = broker.consume([queue], prefetch=1)
  // The reservation is a hash entry keyed by a tag, and an index entry scoring
  // that tag by the time it was claimed — exactly what a Celery worker leaves.
  let indexed = probe.zcount("unacked_index", "0", "+inf") catch { _ => -1L }
  assert_eq(indexed >= 1L, true)
  let held = probe.hget("unacked", delivery.receipt) catch { _ => None }
  let entry = match held {
    Some(bytes) => @json.parse(@utf8.decode_lossy(bytes[:]))
    None => fail("the claim is in the reservation table")
  }
  match entry {
    Array(parts) => {
      assert_eq(parts.length(), 3)
      match parts[2] {
        String(routed) => assert_eq(routed, queue)
        _ => fail("the reservation names its queue")
      }
    }
    _ => fail("a reservation is a triple")
  }
  // The report reads the same table: one message held, none ready. Other tests
  // leave queues behind in the same server, so the report is searched rather
  // than taken from the front.
  let mut reported = None
  for info in broker.queues() {
    if info.name == queue {
      reported = Some(info)
    }
  }
  match reported {
    Some(info) => {
      assert_eq(info.ready, 0L)
      assert_eq(info.unacked, 1L)
    }
    None => fail("the queue is in the report")
  }
  broker.ack(delivery)
  assert_eq(probe.hget("unacked", delivery.receipt) catch { _ => None }, None)
}

///|
async test "recover hands back what a worker that died was holding" {
  guard live(6379) is Some(conn) else { return }
  let clock = Manual::new(1_700_000_000_000L)
  let queue = unique("recover")
  clear(conn, queue)
  // A reservation is left behind as a Celery worker leaves one when it is
  // killed: the message is in `unacked`, the tag in `unacked_index` is scored
  // with the moment it was claimed.
  let broker = celery_broker(conn, clock, visibility=30000L)
  let probe = @moonredis.Client::new(connect() catch { _ => fail("a probe") })
  let message = {
    ..@spec.Message::new("lost-1", "t.step", Json::number(7)),
    queue,
  }
  let reservation = Json::array([
    message.to_celery(),
    Json::string(""),
    Json::string(queue),
  ])
  let tag = "dead-worker:1"
  ignore(
    probe.hset("unacked", [(tag, reservation.stringify())]) catch {
      _ => 0L
    },
  )
  ignore(
    probe.zadd("unacked_index", [
      (tag, (clock.now() - 60000L).to_double() / 1000.0),
    ]) catch {
      _ => 0L
    },
  )
  assert_eq(probe.llen(queue) catch { _ => -1L }, 0L)
  let moved = broker.recover()
  assert_eq(moved, 1L)
  assert_eq(probe.llen(queue) catch { _ => -1L }, 1L)
  // the entry is gone from the table, and what is back on the queue is the
  // message it was holding
  assert_eq(probe.hget("unacked", tag) catch { _ => None }, None)
  let restored = probe.lrange(queue, 0, 0) catch { _ => ([] : Array[Bytes]) }
  match restored {
    [payload] => {
      let back = @spec.Message::from_celery(
        @json.parse(@utf8.decode_lossy(payload[:])) catch {
          _ => fail("the restored payload is an envelope")
        },
        queue~,
      )
      assert_eq(back.id, "lost-1")
      assert_eq(back.args, Json::number(7))
    }
    _ => fail("one message was put back")
  }
  ignore(probe.del([queue]) catch { _ => 0L })
}

///|
async test "a claim that is not yet abandoned is left alone" {
  guard live(6379) is Some(conn) else { return }
  let clock = Manual::new(1_700_000_000_000L)
  let queue = unique("fresh")
  clear(conn, queue)
  let broker = celery_broker(conn, clock, visibility=30000L)
  let probe = @moonredis.Client::new(connect() catch { _ => fail("a probe") })
  ignore(probe.hset("unacked", [("live-worker:1", "[]")]) catch { _ => 0L })
  ignore(
    probe.zadd("unacked_index", [
      ("live-worker:1", clock.now().to_double() / 1000.0),
    ]) catch {
      _ => 0L
    },
  )
  assert_eq(broker.recover(), 0L)
  let still_there = probe.hget("unacked", "live-worker:1") catch { _ => None }
  assert_eq(still_there is Some(_), true)
  ignore(probe.hdel("unacked", ["live-worker:1"]) catch { _ => 0L })
  ignore(probe.zrem("unacked_index", ["live-worker:1"]) catch { _ => 0L })
}

///|
async test "priority decides which list a message is published on" {
  guard live(6379) is Some(conn) else { return }
  let clock = Manual::new(1_700_000_000_000L)
  let queue = unique("pri")
  clear(conn, queue)
  let broker = celery_broker(conn, clock)
  let probe = @moonredis.Client::new(connect() catch { _ => fail("a probe") })
  let low = {
    ..@spec.Message::new("low", "t.step", Json::number(1)),
    queue,
    priority: 0,
  }
  let high = {
    ..@spec.Message::new("high", "t.step", Json::number(2)),
    queue,
    priority: 6,
  }
  broker.publish(queue, low)
  broker.publish(queue, high)
  // Celery splits a queue across its priority steps, and reads the lowest first.
  assert_eq(probe.llen(queue) catch { _ => -1L }, 1L)
  let high_list = "\{queue}\u{06}\u{16}6"
  assert_eq(probe.llen(high_list) catch { _ => -1L }, 1L)
  // and our consumer reads every step, so a message a Celery client published
  // with a priority is not left behind
  let first = broker.consume([queue], prefetch=1)
  broker.ack(first)
  assert_eq(first.message.id, "low")
  let second = broker.consume([queue], prefetch=1)
  broker.ack(second)
  assert_eq(second.message.id, "high")
  ignore(probe.del([queue, high_list]) catch { _ => 0L })
}

///|
async test "a chord of parts run here joins on Celery's own keys" {
  guard live(6379) is Some(conn) else { return }
  let clock = Manual::new(1_700_000_000_000L)
  let broker = celery_broker(conn, clock)
  let store = broker.store()
  let probe = @moonredis.Client::new(connect() catch { _ => fail("a probe") })
  let group = unique("group")
  let callback = @spec.Message::new("cb-\{group}", "t.total", Json::array([]))
  let chord = @spec.Chord::{
    key: group,
    index: 0,
    total: 2,
    ids: [],
    callback,
  }
  // the size is written where Celery reads it, before any part can finish
  assert_eq(store.add(group, 2), 2)
  let first = store.settle(
    chord,
    "part-1",
    @spec.outcome_succeeded(Json::number(10), 0, clock.now()),
  )
  match first {
    @spec.ChordState::Waiting(remaining~) => assert_eq(remaining, 1)
    _ => fail("one part is still to come")
  }
  let second = store.settle(
    { ..chord, index: 1, },
    "part-2",
    @spec.outcome_succeeded(Json::number(32), 0, clock.now()),
  )
  match second {
    @spec.ChordState::Settled(values~, failure~) => {
      assert_eq(values, [Json::number(10), Json::number(32)])
      assert_eq(failure, None)
    }
    _ => fail("the last part closes the chord")
  }
  // the keys are cleared once joined, so the next chord of the same group starts
  // from nothing
  assert_eq(
    probe.zcount(@spec.celery_group_key(group, ".j"), "-inf", "+inf") catch {
      _ => -1L
    },
    0L,
  )
}
