// The broker: publish, claim, settle, and hand a dead consumer's work back.
//
// Two layouts live here. In this library's own, a queue is a list under a
// prefix, a delayed message waits in a sorted set, and what a consumer holds is
// a list of its own kept alive by a lease.
//
// In Celery's layout — `Broker::celery` — the queue is the list Celery reads,
// split by priority, and the reservations are Celery's: one `unacked` hash for
// the whole server plus a sorted set of delivery tags scored by claim time.
// That is what makes the two sides able to take each other's work back: a
// `recover` here restores a Celery worker's expired reservations, and a Celery
// worker's own `restore_visible` restores ours.

///|
/// A queue on redis, and the result store beside it.
///
/// The connection is whatever implements `RedisConn`, so a caller may pass a
/// single connection or one drawn from a pool; every call is one command and no
/// state outside redis is kept between them, which is what lets a worker on one
/// machine settle a message that a worker on another published.
///
/// `consumer` names this broker in the keys it holds work under: two brokers
/// that share a name share their in-flight lists, so a process gives its own
/// name (the pod name, the host and the pid) and a restart gets a fresh one.
pub struct Broker {
  client : @moonredis.Client
  clock : () -> Int64
  keys : Keys
  consumer : String
  /// How long the lease of this consumer lives without a refresh, in
  /// milliseconds. 30000, five missed heartbeats of a six-second loop.
  lease : Int64
  /// How long a claim may stay unsettled before it counts as abandoned, in
  /// milliseconds. Celery's visibility timeout, and the same default: an hour.
  visibility : Int64
  /// Counts the deliveries this broker has made, so each gets a tag of its own.
  mut seq : Int64
}

///|
/// A broker over `conn`, with `clock` as its only source of time.
///
/// `prefix` namespaces every key, so one redis can hold several queues side by
/// side; `consumer` identifies this worker, and defaults to `worker` joined with
/// the clock.
pub fn Broker::new(
  conn : &@moonredis.RedisConn,
  clock~ : () -> Int64,
  prefix~ : String = "moontask",
  consumer~ : String = "worker",
  lease~ : Int64 = 30000L,
  visibility~ : Int64 = 3600000L,
) -> Broker {
  {
    client: @moonredis.Client::new(SerialConn::new(conn)),
    clock,
    keys: Keys::new(prefix),
    consumer: "\{consumer}-\{clock()}",
    lease,
    visibility,
    seq: 0L,
  }
}

///|
/// A broker that shares its queue with a Celery worker: the ready list is the
/// one named after the queue, the payload is Celery's envelope, and what a
/// consumer holds is Celery's `unacked` table.
///
/// A delayed message is put on the queue with its eta in the envelope, exactly
/// as Celery's client does, so a Celery worker sees it and holds it until then.
/// A message this broker takes off the queue before its eta is put in this
/// library's own delay set instead, and published again when it comes due; it is
/// then visible to both sides again.
pub fn Broker::celery(
  conn : &@moonredis.RedisConn,
  clock~ : () -> Int64,
  consumer~ : String = "worker",
  visibility~ : Int64 = 3600000L,
) -> Broker {
  {
    client: @moonredis.Client::new(SerialConn::new(conn)),
    clock,
    keys: Keys::celery(),
    consumer: "\{consumer}-\{clock()}",
    lease: 30000L,
    visibility,
    seq: 0L,
  }
}

///|
/// The name this broker holds its in-flight work under.
pub fn Broker::consumer(self : Broker) -> String {
  self.consumer
}

///|
/// How long a claim may stay unsettled before it counts as abandoned.
pub fn Broker::visibility(self : Broker) -> Int64 {
  self.visibility
}

///|
/// A refusal that says what the server said.
fn refused(error : @moonredis.RedisError) -> @spec.Refused {
  @spec.Refused::Transport(got=@debug.to_string(error))
}

///|
/// The tag this broker gives its next delivery. Celery keys its reservation
/// table by a uuid; anything unique will do, and this one says who took it.
fn Broker::next_tag(self : Broker) -> String {
  self.seq = self.seq + 1L
  "\{self.consumer}:\{(self.clock)()}:\{self.seq}"
}

///|
/// The tag Celery scores a claim with: seconds, as a float, because that is what
/// `time()` gives it and what its own restore compares against.
fn seconds_of(milliseconds : Int64) -> Double {
  milliseconds.to_double() / 1000.0
}

///|
/// Publish `message` on `queue`: now if its eta has come, otherwise the way the
/// layout says a delayed message travels.
pub impl @spec.Broker for Broker with fn publish(
  self : Broker,
  queue : String,
  message : @spec.Message,
) -> Unit raise @spec.Refused {
  let payload = if self.keys.is_celery() {
    message.to_celery().stringify()
  } else {
    message.text()
  }
  let now = (self.clock)()
  let _ = self.client.sadd(self.keys.known(), [queue]) catch {
    error => raise refused(error)
  }
  let future = match message.eta {
    Some(eta) => eta > now
    None => false
  }
  // A Celery client puts an eta message on the queue and lets the worker hold
  // it, so a Celery worker must be able to see ours the same way; this library's
  // own layout keeps it out of the queue until it is due.
  if future && !self.keys.is_celery() {
    let eta = message.eta.unwrap_or(now).to_double()
    let _ = self.client.zadd(self.keys.delayed(queue), [(payload, eta)]) catch {
      error => raise refused(error)
    }
    return
  }
  let _ = self.client.lpush(self.keys.ready_pri(queue, message.priority), [
    payload,
  ]) catch {
    error => raise refused(error)
  }
}

///|
/// Move the messages of `queue` whose eta has come from the delay set to the
/// ready list.
///
/// The removal is the claim: several consumers promote at once, and only the one
/// whose `ZREM` removed the member pushes it, so a due message is published once
/// however many workers are watching.
async fn Broker::promote(
  self : Broker,
  queue : String,
) -> Unit raise @spec.Refused {
  let now = (self.clock)().to_double()
  let due = self.client.zrangebyscore_scores(
    self.keys.delayed(queue),
    "-inf",
    now.to_string(),
    count=64,
  ) catch {
    error => raise refused(error)
  }
  for entry in due {
    let payload = entry.0
    let members : Array[String] = [payload]
    let claimed = self.client.zrem(self.keys.delayed(queue), members) catch {
      error => raise refused(error)
    }
    if claimed == 1L {
      // A message parked by us goes back where a Celery worker looks for it, on
      // the step its priority names.
      let target = if self.keys.is_celery() {
        let decoded = @json.parse(payload) catch { _ => Json::null() }
        let priority = match decoded {
          Object(fields) =>
            match fields.get("properties") {
              Some(Object(properties)) =>
                match properties.get("priority") {
                  Some(Number(value, ..)) => value.to_int()
                  _ => 0
                }
              _ => 0
            }
          _ => 0
        }
        self.keys.ready_pri(queue, priority)
      } else {
        self.keys.ready(queue)
      }
      let _ = self.client.lpush(target, members) catch {
        error => raise refused(error)
      }
    }
  }
}

///|
/// Say this consumer is alive, so `recover` leaves its work alone.
async fn Broker::heartbeat(self : Broker) -> Unit raise @spec.Refused {
  let _ = self.client.set(self.keys.lease(self.consumer), "1", px=self.lease) catch {
    error => raise refused(error)
  }
}

///|
/// Take the next message of `queues`, moving it onto this consumer's in-flight
/// list in the same step.
///
/// `prefetch` bounds how many messages this consumer holds: a consumer already
/// holding that many waits, so a slow body cannot bury the others in a queue of
/// claims. A prefetch of zero or less means no bound. Celery's layout does not
/// bound it here: the reservation table there is not per consumer, and the
/// worker asks for one message at a time anyway.
pub impl @spec.Broker for Broker with fn consume(
  self : Broker,
  queues : ArrayView[String],
  prefetch~ : Int32,
) -> @spec.Delivery raise @spec.Refused {
  self.heartbeat()
  while true {
    for queue in queues {
      self.promote(queue)
    }
    if self.keys.is_celery() {
      match self.claim_celery(queues) {
        Some(delivery) => return delivery
        None => ()
      }
    } else {
      match self.claim_own(queues, prefetch~) {
        Some(delivery) => return delivery
        None => ()
      }
    }
    // Nothing was ready anywhere. Come back in a few milliseconds rather than
    // block on the server: the connection is shared, and a command that waits
    // inside it would keep every other task out.
    @wait.nap(5)
  } nobreak {
    raise @spec.Refused::Missing(kind="broker")
  }
}

///|
/// Take one message from this library's own layout: the ready list under the
/// prefix, and the message moved onto this consumer's in-flight list.
async fn Broker::claim_own(
  self : Broker,
  queues : ArrayView[String],
  prefetch~ : Int32,
) -> @spec.Delivery? raise @spec.Refused {
  for queue in queues {
    let inflight = self.keys.inflight(self.consumer, queue)
    if prefetch > 0 {
      let held = self.client.llen(inflight) catch {
        error => raise refused(error)
      }
      if held >= prefetch.to_int64() {
        continue
      }
    }
    let payload = self.client.rpoplpush(self.keys.ready(queue), inflight) catch {
      error => raise refused(error)
    }
    match payload {
      Some(payload) => {
        let text = @utf8.decode_lossy(payload[:])
        let parsed : Result[@spec.Message, @spec.Refused] = Ok(
          @spec.Message::parse(text),
        ) catch {
          error => Err(error)
        }
        let message = match parsed {
          Ok(message) => message
          Err(error) => {
            self.unclaim(queue, text)
            raise error
          }
        }
        return Some({ message, queue, receipt: text, })
      }
      None => ()
    }
  }
  None
}

///|
/// Take one message from Celery's layout: popped off the priority list Celery
/// reads first, and written into the `unacked` hash and its index in the same
/// breath, which is what a Celery worker does when it claims one.
///
/// A message whose eta has not come is put into this library's delay set rather
/// than run early; it goes back on the queue when it is due.
async fn Broker::claim_celery(
  self : Broker,
  queues : ArrayView[String],
) -> @spec.Delivery? raise @spec.Refused {
  let now = (self.clock)()
  for queue in queues {
    for step in priority_steps() {
      let payload = self.client.rpop(self.keys.ready_pri(queue, step)) catch {
        error => raise refused(error)
      }
      let bytes = match payload {
        Some(bytes) => bytes
        None => continue
      }
      let text = @utf8.decode_lossy(bytes[:])
      let decoded = @json.parse(text) catch {
        _ => {
          self.unclaim(queue, text)
          raise @spec.Refused::Malformed(got=text)
        }
      }
      let parsed : Result[@spec.Message, @spec.Refused] = Ok(
        @spec.Message::from_celery(decoded, queue~),
      ) catch {
        error => Err(error)
      }
      let message = match parsed {
        Ok(message) => message
        Err(error) => {
          self.unclaim(queue, text)
          raise error
        }
      }
      if !message.is_due(now) {
        let eta = message.eta.unwrap_or(now).to_double()
        let _ = self.client.zadd(self.keys.delayed(queue), [(text, eta)]) catch {
          error => raise refused(error)
        }
        continue
      }
      let tag = self.next_tag()
      let reservation = Json::array([
        decoded,
        Json::string(""),
        Json::string(queue),
      ])
      let _ = self.client.hset(unacked(), [(tag, reservation.stringify())]) catch {
        error => raise refused(error)
      }
      let _ = self.client.zadd(unacked_index(), [(tag, seconds_of(now))]) catch {
        error => raise refused(error)
      }
      return Some({ message, queue, receipt: tag, })
    }
  }
  None
}

///|
/// Put a payload that could not be decoded back where it came from, so an
/// operator can look at it rather than lose it.
async fn Broker::unclaim(
  self : Broker,
  queue : String,
  text : String,
) -> Unit raise @spec.Refused {
  let _ = self.client.lpush(self.keys.ready(queue), [text]) catch {
    error => raise refused(error)
  }
}

///|
/// Settle `delivery`: it will not be handed out again.
pub impl @spec.Broker for Broker with fn ack(
  self : Broker,
  delivery : @spec.Delivery,
) -> Unit raise @spec.Refused {
  if self.keys.is_celery() {
    self.forget(delivery.receipt)
    return
  }
  let _ = self.client.lrem(
    self.keys.inflight(self.consumer, delivery.queue),
    1,
    delivery.receipt,
  ) catch {
    error => raise refused(error)
  }
}

///|
/// Drop one reservation from Celery's table: the hash entry and its index.
async fn Broker::forget(
  self : Broker,
  tag : String,
) -> Unit raise @spec.Refused {
  let _ = self.client.hdel(unacked(), [tag]) catch {
    error => raise refused(error)
  }
  let _ = self.client.zrem(unacked_index(), [tag]) catch {
    error => raise refused(error)
  }
}

///|
/// Settle `delivery` by putting it back, or by dropping it.
///
/// A requeued message whose eta has not come is published again rather than run
/// straight away, so a rejection cannot jump the schedule.
pub impl @spec.Broker for Broker with fn reject(
  self : Broker,
  delivery : @spec.Delivery,
  requeue~ : Bool,
) -> Unit raise @spec.Refused {
  if self.keys.is_celery() {
    self.forget(delivery.receipt)
  } else {
    let _ = self.client.lrem(
      self.keys.inflight(self.consumer, delivery.queue),
      1,
      delivery.receipt,
    ) catch {
      error => raise refused(error)
    }
  }
  if requeue {
    self.publish(delivery.queue, delivery.message)
  }
}

///|
/// Give back everything a consumer that stopped without settling was holding.
///
/// In this library's own layout a consumer that dies leaves its messages on its
/// in-flight list and stops refreshing its lease; once the lease expires this
/// hands those messages back to their queues. In Celery's layout the table is
/// Celery's — one hash, one index scored by claim time — so this restores what a
/// Celery worker was holding when it died, using the same visibility timeout
/// Celery does, and a Celery worker restores ours the same way.
///
/// Run it from a supervisor or a cron entry; it is safe to run from several
/// places at once, since the two would only put the same message back.
pub async fn Broker::recover(self : Broker) -> Int64 raise @spec.Refused {
  let mut moved = 0L
  if self.keys.is_celery() {
    moved = moved + self.recover_celery()
  }
  // Every lease that is still there is a live consumer; every in-flight list
  // whose consumer has none belongs to a consumer that is gone.
  let live = Map([])
  let mut cursor = "0"
  while true {
    let (next, keys) = self.client.scan(
      cursor,
      pattern=self.keys.lease_pattern(),
      count=128,
      kind="string",
    ) catch {
      error => raise refused(error)
    }
    for key in keys {
      live[self.keys.consumer_of_lease(key)] = ()
    }
    cursor = next
    if cursor == "0" {
      break
    }
  }
  let mut cursor = "0"
  while true {
    let (next, keys) = self.client.scan(
      cursor,
      pattern=self.keys.inflight_pattern(),
      count=128,
      kind="list",
    ) catch {
      error => raise refused(error)
    }
    for key in keys {
      let consumer = self.keys.consumer_of_inflight(key)
      if live.contains(consumer) {
        continue
      }
      let queue = self.queue_of_inflight(key)
      let held = self.client.lrange(key, 0, -1) catch {
        error => raise refused(error)
      }
      for payload in held {
        let text = @utf8.decode_lossy(payload[:])
        let _ = self.client.rpush(self.keys.ready(queue), [text]) catch {
          error => raise refused(error)
        }
        moved = moved + 1L
      }
      let _ = self.client.del([key]) catch { error => raise refused(error) }
    }
    cursor = next
    if cursor == "0" {
      break
    }
  }
  moved
}

///|
/// Restore the reservations Celery's table says have been held too long.
///
/// The mutex is Celery's too, so a Celery worker restoring at the same moment
/// and this broker do not both put the same message back.
async fn Broker::recover_celery(self : Broker) -> Int64 raise @spec.Refused {
  let mutex = unacked_mutex()
  let held = self.client.set(mutex, "1", px=5000L, nx=true) catch {
    error => raise refused(error)
  }
  if !held {
    return 0L
  }
  let mut moved = 0L
  let ceiling = seconds_of((self.clock)() - self.visibility)
  let expired = self.client.zrangebyscore_scores(
    unacked_index(),
    "-inf",
    ceiling.to_string(),
    count=64,
  ) catch {
    error => {
      let _ = self.client.del([mutex]) catch { _ => 0L }
      raise refused(error)
    }
  }
  for entry in expired {
    let tag = entry.0
    let reservation = self.client.hget(unacked(), tag) catch {
      error => raise refused(error)
    }
    match reservation {
      Some(bytes) => {
        let text = @utf8.decode_lossy(bytes[:])
        self.restore(text)
        moved = moved + 1L
      }
      None => ()
    }
    self.forget(tag)
  }
  let _ = self.client.del([mutex]) catch { _ => 0L }
  moved
}

///|
/// Put one reserved message back on the queue it names.
///
/// A reservation is `[message, exchange, routing_key]`; the message is the
/// envelope as it arrived, and it goes back with `redelivered` set, which is
/// what a Celery worker's own restore does.
async fn Broker::restore(
  self : Broker,
  text : String,
) -> Unit raise @spec.Refused {
  let entry = @json.parse(text) catch {
    _ => raise @spec.Refused::Malformed(got=text)
  }
  let parts = match entry {
    Array(parts) => parts
    _ => raise @spec.Refused::Malformed(got="a reservation is a triple")
  }
  if parts.length() < 3 {
    raise @spec.Refused::Malformed(got="a reservation is a triple")
  }
  let raw = match parts[0] {
    Object(fields) => {
      let headers = match fields.get("headers") {
        Some(Object(headers)) => headers
        _ => Map([])
      }
      headers["redelivered"] = Json::boolean(true)
      fields["headers"] = Json::object(headers)
      Json::object(fields)
    }
    other => other
  }
  let queue = match parts[2] {
    String(queue) => queue
    _ => raise @spec.Refused::Malformed(got="a reservation names no queue")
  }
  let priority = match raw {
    Object(fields) =>
      match fields.get("properties") {
        Some(Object(properties)) =>
          match properties.get("priority") {
            Some(Number(value, ..)) => value.to_int()
            _ => 0
          }
        _ => 0
      }
    _ => 0
  }
  // `rpush`: a restored message is the next one to be taken, which is where
  // Celery's own restore puts it too.
  let _ = self.client.rpush(self.keys.ready_pri(queue, priority), [
    raw.stringify(),
  ]) catch {
    error => raise refused(error)
  }
}

///|
/// The queue an in-flight list belongs to, which is the part of its key after
/// the consumer.
fn Broker::queue_of_inflight(self : Broker, key : String) -> String {
  let rest = strip_prefix(key, "\{self.keys.prefix}:p:")
  match rest[:].find(":") {
    Some(at) => rest.unsafe_substring(start=at + 1, end=rest.length())
    None => "default"
  }
}

///|
pub extend Broker with @spec.Broker::{publish, consume, ack, reject}
