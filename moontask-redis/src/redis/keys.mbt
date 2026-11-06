// Where the broker keeps things.
//
// One prefix and a fixed set of suffixes, so an operator can read a queue with
// `redis-cli` without reading this file: `q:` is what is ready, `d:` what is
// waiting for its eta, and `p:` what a consumer holds and has not settled.
//
// A queue is a list because a list is the one structure redis pops from in
// order and pushes to in order; a delayed message is a member of a sorted set
// scored by its eta, because that is the structure that answers "what is due"
// without reading everything.
//
// In Celery's layout the names are Celery's, not this file's, and that is what
// lets a queue written here be read there: the ready lists are named after the
// queue and split by priority, and what every consumer holds is one hash for the
// whole server rather than a list per consumer.

///|
/// The key names one broker uses, under its prefix.
pub struct Keys {
  prefix : String
  /// Whether the queue keys are Celery's — the queue's own name, no prefix — and
  /// the reservations live in Celery's `unacked` hash. That is the whole of the
  /// difference between a broker of ours and one a Celery worker can share.
  celery : Bool
}

///|
fn Keys::new(prefix : String) -> Keys {
  { prefix, celery: false, }
}

///|
/// The keys a Celery worker and this broker agree on: the queue is the list
/// named after it, and what is held unacknowledged is Celery's own table.
fn Keys::celery() -> Keys {
  { prefix: "moontask", celery: true, }
}

///|
fn Keys::is_celery(self : Keys) -> Bool {
  self.celery
}

///|
/// The list a queue is ready on.
fn Keys::ready(self : Keys, queue : String) -> String {
  if self.celery {
    queue
  } else {
    "\{self.prefix}:q:\{queue}"
  }
}

///|
/// The list a queue is ready on for one priority.
///
/// Celery splits a queue across the priority steps its transport was configured
/// with — the queue name, then the name and a separator and the step — and reads
/// them from the lowest step up. A message of priority `p` belongs on the step at
/// or below `p`; a broker that ignored the split would publish everything onto
/// the step Celery reads first, and would never see a message a Celery client
/// published with a priority.
fn Keys::ready_pri(self : Keys, queue : String, priority : Int32) -> String {
  if !self.celery {
    return self.ready(queue)
  }
  let step = priority_step(priority)
  if step == 0 {
    queue
  } else {
    "\{queue}\u{06}\u{16}\{step}"
  }
}

///|
/// The priority steps Celery's redis transport uses, lowest first. A priority is
/// rounded down to one of them.
fn priority_steps() -> Array[Int32] {
  [0, 3, 6, 9]
}

///|
/// The step a priority belongs on: the highest one at or below it.
fn priority_step(priority : Int32) -> Int32 {
  let mut chosen = 0
  for step in priority_steps() {
    if priority >= step {
      chosen = step
    }
  }
  chosen
}

///|
/// The hash Celery keeps every consumer's unacknowledged messages in, keyed by
/// delivery tag, with the value `[message, exchange, routing_key]` as JSON.
///
/// It is one hash for every consumer on the server, not one per consumer: what
/// decides whether a reservation is still alive is the claim time in
/// `unacked_index` and the visibility timeout, not a lease of our own. Sharing
/// it is what lets a worker here hand back the work of a Celery worker that
/// died, and lets a Celery worker hand back ours.
fn unacked() -> String {
  "unacked"
}

///|
/// The sorted set of delivery tags Celery scores by the time they were claimed.
fn unacked_index() -> String {
  "unacked_index"
}

///|
/// The lock Celery takes while it restores expired reservations.
fn unacked_mutex() -> String {
  "unacked_mutex"
}

///|
/// The sorted set of a queue's messages waiting for their eta, in this library's
/// own layout.
fn Keys::delayed(self : Keys, queue : String) -> String {
  "\{self.prefix}:d:\{queue}"
}

///|
/// The list one consumer holds unacknowledged messages on, in this library's own
/// layout.
fn Keys::inflight(self : Keys, consumer : String, queue : String) -> String {
  "\{self.prefix}:p:\{consumer}:\{queue}"
}

///|
/// The key that says a consumer is alive, and expires when it stops.
fn Keys::lease(self : Keys, consumer : String) -> String {
  "\{self.prefix}:c:\{consumer}"
}

///|
/// The set of queue names that have been used under this prefix.
///
/// redis drops a list key when its last element goes, so a drained queue leaves
/// no trace in the keyspace; the set is what lets a report still name it.
fn Keys::known(self : Keys) -> String {
  "\{self.prefix}:queues"
}

///|
/// The key a result is stored under.
fn Keys::result(self : Keys, id : String) -> String {
  if self.celery {
    @spec.celery_result_key(id)
  } else {
    "\{self.prefix}:r:\{id}"
  }
}

///|
/// The key a chord counts its parts under, in this library's own layout.
fn Keys::chord(self : Keys, key : String) -> String {
  "\{self.prefix}:h:\{key}"
}

///|
/// The pattern that matches every ready list.
fn Keys::ready_pattern(self : Keys) -> String {
  "\{self.prefix}:q:*"
}

///|
/// The pattern that matches every unacknowledged list.
fn Keys::inflight_pattern(self : Keys) -> String {
  "\{self.prefix}:p:*"
}

///|
/// The pattern that matches every lease.
fn Keys::lease_pattern(self : Keys) -> String {
  "\{self.prefix}:c:*"
}

///|
/// The queue name a ready list key stands for.
fn Keys::queue_of_ready(self : Keys, key : String) -> String {
  strip_prefix(key, "\{self.prefix}:q:")
}

///|
/// The consumer a lease key belongs to.
fn Keys::consumer_of_lease(self : Keys, key : String) -> String {
  strip_prefix(key, "\{self.prefix}:c:")
}

///|
/// The consumer an unacknowledged list key belongs to.
fn Keys::consumer_of_inflight(self : Keys, key : String) -> String {
  let rest = strip_prefix(key, "\{self.prefix}:p:")
  match rest[:].find(":") {
    Some(at) => rest.unsafe_substring(start=0, end=at)
    None => rest
  }
}

///|
fn strip_prefix(text : String, prefix : String) -> String {
  if text.has_prefix(prefix[:]) {
    text.unsafe_substring(start=prefix.length(), end=text.length())
  } else {
    text
  }
}
