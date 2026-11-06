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

///|
/// The key names one broker uses, under its prefix.
pub struct Keys {
  prefix : String
  /// Whether the ready key is Celery's — the queue's own name, no prefix — while
  /// everything else keeps this prefix. That is the whole of the difference
  /// between a broker of ours and one a Celery worker can share.
  celery : Bool
}

///|
fn Keys::new(prefix : String) -> Keys {
  { prefix, celery: false, }
}

///|
/// The keys a Celery worker and this broker agree on: the queue is the list
/// named after it, and the delay set and in-flight list are ours, under
/// `moontask`.
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
/// The sorted set of a queue's messages waiting for their eta.
fn Keys::delayed(self : Keys, queue : String) -> String {
  "\{self.prefix}:d:\{queue}"
}

///|
/// The list one consumer holds unacknowledged messages on.
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
/// The key a chord counts its parts under.
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
