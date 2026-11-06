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
  let text = if self.keys.is_celery() {
    outcome.to_celery(id).stringify()
  } else {
    Json(outcome).stringify()
  }
  // Celery's own expiry for a result, so a Python client and this store agree on
  // how long an answer is kept.
  let _ = self.client.set(self.keys.result(id), text, px=86400000L) catch {
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
      if self.keys.is_celery() {
        return Some(
          @spec.Outcome::from_celery(
            @json.parse(text) catch {
              _ => raise @spec.Refused::Malformed(got=text)
            },
          ),
        )
      }
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
/// Open a chord, or count one part.
///
/// In Celery's layout the counter is Celery's own: a chord of `n` parts is the
/// key `.s` beside the group's results, and the results themselves are written by
/// `settle` as the parts arrive. A part is never counted here, only the opening
/// is, which is why a negative delta answers zero.
pub impl @spec.Counter for Store with fn add(
  self : Store,
  key : String,
  delta : Int32,
) -> Int32 raise @spec.Refused {
  if self.keys.is_celery() {
    if delta > 0 {
      let _ = self.client.set(
        @spec.celery_group_key(key, ".s"),
        delta.to_string(),
      ) catch {
        error => raise @spec.Refused::Transport(got=@debug.to_string(error))
      }
      ignore(
        self.client.del([
          @spec.celery_group_key(key, ".j"),
          @spec.celery_group_key(key, ".t"),
        ]) catch {
          error => raise @spec.Refused::Transport(got=@debug.to_string(error))
        },
      )
    }
    return delta
  }
  let next = self.client.incrby(self.keys.chord(key), delta.to_int64()) catch {
    error => raise @spec.Refused::Transport(got=@debug.to_string(error))
  }
  next.to_int()
}

///|
pub impl @spec.Counter for Store with fn drop(self : Store, key : String) -> Unit raise @spec.Refused {
  let doomed = if self.keys.is_celery() {
    [
      @spec.celery_group_key(key, ".j"),
      @spec.celery_group_key(key, ".t"),
      @spec.celery_group_key(key, ".s"),
    ]
  } else {
    [self.keys.chord(key)]
  }
  let _ = self.client.del(doomed) catch {
    error => raise @spec.Refused::Transport(got=@debug.to_string(error))
  }
}

///|
/// Count a settled part, and when it was the last, hand back the parts' results.
///
/// In Celery's layout this is the join Celery itself uses: the part's result goes
/// into the group's `.j` sorted set, scored by its index, and the part that
/// brings the count up to the chord's size reads the set in index order and
/// clears the three keys. A part run by a Celery worker writes the same three
/// keys, so a chord may be split across the boundary and only the last part — on
/// either side — closes it.
pub impl @spec.Counter for Store with fn settle(
  self : Store,
  chord : @spec.Chord,
  id : String,
  outcome : @spec.Outcome,
) -> @spec.ChordState raise @spec.Refused {
  if !self.keys.is_celery() {
    let remaining = self.add(chord.key, -1)
    if remaining > 0 {
      return @spec.ChordState::Waiting(remaining~)
    }
    self.drop(chord.key)
    return self.gather(chord, outcome)
  }
  let entry = Json::array([
    Json::number(1),
    Json::string(id),
    Json::string(outcome.state.code()),
    match outcome.result {
      Some(value) => value
      None => Json::null()
    },
  ])
  let index = chord.index
  let _ = self.client.zadd(@spec.celery_group_key(chord.key, ".j"), [
    (entry.stringify(), index.to_double()),
  ]) catch {
    error => raise @spec.Refused::Transport(got=@debug.to_string(error))
  }
  let ready = self.client.zcount(
    @spec.celery_group_key(chord.key, ".j"),
    "-inf",
    "+inf",
  ) catch {
    error => raise @spec.Refused::Transport(got=@debug.to_string(error))
  }
  let total = match self.chord_size(chord) {
    Some(total) => total
    None => chord.total.to_int64()
  }
  if ready < total {
    return @spec.ChordState::Waiting(remaining=(total - ready).to_int())
  }
  let entries = self.client.zrange(
    @spec.celery_group_key(chord.key, ".j"),
    0,
    -1,
  ) catch {
    error => raise @spec.Refused::Transport(got=@debug.to_string(error))
  }
  self.drop(chord.key)
  let values = []
  let mut failure = None
  for text in entries {
    let decoded = @json.parse(text) catch { _ => Json::null() }
    let parts = match decoded {
      Array(parts) => parts
      _ => []
    }
    let state = match parts.length() > 2 {
      true =>
        match parts[2] {
          String(state) => state
          _ => ""
        }
      false => ""
    }
    if state == "SUCCESS" {
      values.push(
        match parts.length() > 3 {
          true => parts[3]
          false => Json::null()
        },
      )
    } else if failure is None {
      failure = Some(if state == "" { "part did not finish" } else { state })
    }
  }
  @spec.ChordState::Settled(values~, failure~)
}

///|
/// The size Celery recorded for a chord, when it recorded one.
async fn Store::chord_size(
  self : Store,
  chord : @spec.Chord,
) -> Int64? raise @spec.Refused {
  let held = self.client.get(@spec.celery_group_key(chord.key, ".s")) catch {
    error => raise @spec.Refused::Transport(got=@debug.to_string(error))
  }
  match held {
    Some(bytes) => {
      let text = @utf8.decode_lossy(bytes[:])
      let size = @string.parse_int64(text[:]) catch { _ => return None }
      Some(size)
    }
    None => None
  }
}

///|
/// Collect a chord's parts from the store, in the order the parts were declared.
async fn Store::gather(
  self : Store,
  chord : @spec.Chord,
  outcome : @spec.Outcome,
) -> @spec.ChordState raise @spec.Refused {
  let values = []
  let mut failure = None
  for part in chord.ids {
    match self.fetch(part) {
      Some(done) =>
        match done.state {
          @spec.Succeeded => values.push(done.result.unwrap_or(Json::null()))
          @spec.Failed =>
            if failure is None {
              failure = Some(done.error.unwrap_or("failed"))
            }
          _ => if failure is None { failure = Some("part did not finish") }
        }
      None => if failure is None { failure = Some("part did not finish") }
    }
  }
  if failure is None {
    match outcome.state {
      @spec.Failed => failure = Some(outcome.error.unwrap_or("failed"))
      @spec.Revoked => failure = Some("part did not finish")
      _ => ()
    }
  }
  @spec.ChordState::Settled(values~, failure~)
}

///|
pub extend Store with @spec.Backend::{store, fetch}

///|
pub extend Store with @spec.Counter::{add, drop, settle}

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
  // In Celery's layout a consumer's reservations are not named after a queue, so
  // the hash is read once and counted per routing key.
  let held_by_queue = Map([])
  if self.keys.is_celery() {
    let held = self.client.hgetall(unacked()) catch {
      error => raise @spec.Refused::Transport(got=@debug.to_string(error))
    }
    for entry in held {
      let decoded = @json.parse(entry.1) catch { _ => Json::null() }
      let parts = match decoded {
        Array(parts) => parts
        _ => []
      }
      if parts.length() > 2 {
        match parts[2] {
          String(queue) =>
            held_by_queue[queue] = held_by_queue.get(queue).unwrap_or(0L) + 1L
          _ => ()
        }
      }
    }
  }
  let info = []
  for name, _ in names {
    let ready = if self.keys.is_celery() {
      let mut total = 0L
      for step in priority_steps() {
        total = total +
          (self.client.llen(self.keys.ready_pri(name, step)) catch {
            error => raise @spec.Refused::Transport(got=@debug.to_string(error))
          })
      }
      total
    } else {
      self.client.llen(self.keys.ready(name)) catch {
        error => raise @spec.Refused::Transport(got=@debug.to_string(error))
      }
    }
    let delayed = self.client.zcard(self.keys.delayed(name)) catch {
      error => raise @spec.Refused::Transport(got=@debug.to_string(error))
    }
    // Unacknowledged messages are counted across every consumer, since which
    // consumer holds one is a detail of who is running, not of the queue.
    let mut unacked = held_by_queue.get(name).unwrap_or(0L)
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
