// The store, the chord counter and the queue report, all in the same keyspace as
// the queues.

///|
/// The result store and the chord counter of a redis broker.
///
/// They are the same object because they are the same keyspace and the same
/// connection: a worker stores a result and counts a chord part on the way out of
/// the same function.
pub struct Store {
  client : @moonredis.Client
  keys : Keys
}

///|
/// A store over the same connection and prefix as `broker`.
pub fn Store::new(
  conn : &@moonredis.RedisConn,
  prefix~ : String = "moontask",
) -> Store {
  {
    client: @moonredis.Client::new(SerialConn::new(conn)),
    keys: Keys::new(prefix),
  }
}

///|
/// A store beside a broker, sharing its prefix.
pub fn Broker::store(self : Broker) -> Store {
  { client: self.client, keys: self.keys, }
}

///|
pub impl @spec.Backend for Store with fn store(
  self : Store,
  id : String,
  outcome : @spec.Outcome,
) -> Unit raise @spec.Refused {
  let text = Json(outcome).stringify()
  let _ = self.client.set(self.keys.result(id), text) catch {
    error => raise @spec.Refused::Transport(got=@debug.to_string(error))
  }
}

///|
pub impl @spec.Backend for Store with fn fetch(self : Store, id : String) -> @spec.Outcome? raise @spec.Refused {
  let value = self.client.get(self.keys.result(id)) catch {
    error => raise @spec.Refused::Transport(got=@debug.to_string(error))
  }
  match value {
    Some(bytes) => {
      let text = @utf8.decode_lossy(bytes[:])
      let outcome : @spec.Outcome = @json.from_json(
        @json.parse(text) catch {
          _ => raise @spec.Refused::Malformed(got=text)
        },
      ) catch {
        _ => raise @spec.Refused::Malformed(got=text)
      }
      Some(outcome)
    }
    None => None
  }
}

///|
pub impl @spec.Counter for Store with fn add(
  self : Store,
  key : String,
  delta : Int32,
) -> Int32 raise @spec.Refused {
  let next = self.client.incrby(self.keys.chord(key), delta.to_int64()) catch {
    error => raise @spec.Refused::Transport(got=@debug.to_string(error))
  }
  next.to_int()
}

///|
pub impl @spec.Counter for Store with fn drop(self : Store, key : String) -> Unit raise @spec.Refused {
  let _ = self.client.del([self.keys.chord(key)]) catch {
    error => raise @spec.Refused::Transport(got=@debug.to_string(error))
  }
}

///|
pub extend Store with @spec.Backend::{store, fetch}

///|
pub extend Store with @spec.Counter::{add, drop}

///|
/// Every queue under the prefix, with what waits in each.
///
/// The three key families are scanned rather than kept in an index: a queue that
/// was written by another process must show up without that process having
/// registered it anywhere.
pub impl @spec.Inspector for Broker with fn queues(self : Broker) -> Array[
  @spec.QueueInfo,
] raise @spec.Refused {
  let names = Map([])
  let known = self.client.smembers(self.keys.known()) catch {
    error => raise @spec.Refused::Transport(got=@debug.to_string(error))
  }
  for queue in known {
    names[queue] = ()
  }
  let mut cursor = "0"
  while true {
    let (next, keys) = self.client.scan(
      cursor,
      pattern=self.keys.ready_pattern(),
      count=256,
      kind="list",
    ) catch {
      error => raise @spec.Refused::Transport(got=@debug.to_string(error))
    }
    for key in keys {
      names[self.keys.queue_of_ready(key)] = ()
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
      count=256,
      kind="list",
    ) catch {
      error => raise @spec.Refused::Transport(got=@debug.to_string(error))
    }
    for key in keys {
      names[self.queue_of_inflight(key)] = ()
    }
    cursor = next
    if cursor == "0" {
      break
    }
  }
  let info = []
  for name, _ in names {
    let ready = self.client.llen(self.keys.ready(name)) catch {
      error => raise @spec.Refused::Transport(got=@debug.to_string(error))
    }
    let delayed = self.client.zcard(self.keys.delayed(name)) catch {
      error => raise @spec.Refused::Transport(got=@debug.to_string(error))
    }
    // Unacknowledged messages are counted across every consumer, since which
    // consumer holds one is a detail of who is running, not of the queue.
    let mut unacked = 0L
    let mut cursor = "0"
    while true {
      let (next, keys) = self.client.scan(
        cursor,
        pattern=self.keys.inflight_pattern(),
        count=256,
        kind="list",
      ) catch {
        error => raise @spec.Refused::Transport(got=@debug.to_string(error))
      }
      for key in keys {
        if self.queue_of_inflight(key) == name {
          let held = self.client.llen(key) catch {
            error => raise @spec.Refused::Transport(got=@debug.to_string(error))
          }
          unacked = unacked + held
        }
      }
      cursor = next
      if cursor == "0" {
        break
      }
    }
    info.push(@spec.QueueInfo::{ name, ready, delayed, unacked, })
  }
  info
}

///|
/// Up to `limit` messages that would be taken next from `queue`, without
/// claiming them.
pub impl @spec.Inspector for Broker with fn peek(
  self : Broker,
  queue : String,
  limit~ : Int32,
) -> Array[@spec.Message] raise @spec.Refused {
  let held = self.client.lrange(self.keys.ready(queue), 0, limit - 1) catch {
    error => raise @spec.Refused::Transport(got=@debug.to_string(error))
  }
  // The list is pushed at the head and taken from the tail, so the messages
  // that would be taken next are the last ones in the range: read the range
  // back to front.
  let messages = []
  let mut at = held.length() - 1
  while at >= 0 {
    let text = @utf8.decode_lossy(held[at][:])
    messages.push(@spec.Message::parse(text))
    at = at - 1
  }
  messages
}

///|
pub extend Broker with @spec.Inspector::{queues, peek}
