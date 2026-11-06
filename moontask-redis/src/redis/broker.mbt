// The broker: publish, claim, settle, and hand a dead consumer's work back.

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
) -> Broker {
  {
    client: @moonredis.Client::new(SerialConn::new(conn)),
    clock,
    keys: Keys::new(prefix),
    consumer: "\{consumer}-\{clock()}",
    lease,
  }
}

///|
/// A broker that shares its ready queue with a Celery worker: the queue is the
/// list named after it, and the payload is Celery's envelope.
///
/// The delayed set and the in-flight lists stay under this library's own prefix,
/// because Celery's redis transport keeps neither on the server — a worker there
/// holds an eta message in memory, and a reservation in one hash for all
/// consumers.
pub fn Broker::celery(
  conn : &@moonredis.RedisConn,
  clock~ : () -> Int64,
  consumer~ : String = "worker",
  lease~ : Int64 = 30000L,
) -> Broker {
  {
    client: @moonredis.Client::new(SerialConn::new(conn)),
    clock,
    keys: Keys::celery(),
    consumer: "\{consumer}-\{clock()}",
    lease,
  }
}

///|
/// The name this broker holds its in-flight work under.
pub fn Broker::consumer(self : Broker) -> String {
  self.consumer
}

///|
/// A refusal that says what the server said.
fn refused(error : @moonredis.RedisError) -> @spec.Refused {
  @spec.Refused::Transport(got=@debug.to_string(error))
}

///|
/// Publish `message` on `queue`: now if its eta has come, otherwise into the
/// sorted set that holds it until then.
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
  if future {
    let eta = message.eta.unwrap_or(now).to_double()
    let _ = self.client.zadd(self.keys.delayed(queue), [(payload, eta)]) catch {
      error => raise refused(error)
    }
  } else {
    let _ = self.client.lpush(self.keys.ready(queue), [payload]) catch {
      error => raise refused(error)
    }
  }
}

///|
/// Move the messages of `queue` whose eta has come from the sorted set to the
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
      let _ = self.client.lpush(self.keys.ready(queue), members) catch {
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
/// claims. A prefetch of zero or less means no bound.
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
          // A payload that does not decode is not a message this worker can
          // run: it goes back as it came, so an operator can look at it rather
          // than losing it, and the caller hears why the claim failed.
          let parsed : Result[@spec.Message, @spec.Refused] = if self.keys.is_celery() {
            let decoded = @json.parse(text) catch {
              _ => raise @spec.Refused::Malformed(got=text)
            }
            Ok(@spec.Message::from_celery(decoded, queue~)) catch {
              error => Err(error)
            }
          } else {
            Ok(@spec.Message::parse(text)) catch {
              error => Err(error)
            }
          }
          match parsed {
            Ok(message) => return { message, queue, receipt: text, }
            Err(error) => {
              let _ = self.client.lpush(self.keys.ready(queue), [text]) catch {
                inner => raise refused(inner)
              }
              raise error
            }
          }
        }
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
/// Settle `delivery`: it will not be handed out again.
pub impl @spec.Broker for Broker with fn ack(
  self : Broker,
  delivery : @spec.Delivery,
) -> Unit raise @spec.Refused {
  let _ = self.client.lrem(
    self.keys.inflight(self.consumer, delivery.queue),
    1,
    delivery.receipt,
  ) catch {
    error => raise refused(error)
  }
}

///|
/// Settle `delivery` by putting it back, or by dropping it.
///
/// A requeued message whose eta has not come goes back into the delayed set
/// rather than the ready list, so a rejection cannot jump the schedule.
pub impl @spec.Broker for Broker with fn reject(
  self : Broker,
  delivery : @spec.Delivery,
  requeue~ : Bool,
) -> Unit raise @spec.Refused {
  let _ = self.client.lrem(
    self.keys.inflight(self.consumer, delivery.queue),
    1,
    delivery.receipt,
  ) catch {
    error => raise refused(error)
  }
  if requeue {
    self.publish(delivery.queue, delivery.message)
  }
}

///|
/// Give back everything a consumer that stopped without settling was holding.
///
/// A consumer that dies leaves its messages on its in-flight list and stops
/// refreshing its lease; once the lease expires this hands those messages back
/// to their queues, which is what makes a killed worker's work run again. It
/// answers how many messages it moved.
///
/// Run it from a supervisor or a cron entry; it is safe to run from several
/// places at once, since the two would only put the same message back.
pub async fn Broker::recover(self : Broker) -> Int64 raise @spec.Refused {
  let mut moved = 0L
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
        let _ = self.client.lpush(self.keys.ready(queue), [text]) catch {
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
