// The message a client publishes and a worker consumes. This file is the data
// shape; `wire.mbt` is its JSON form.

///|
/// One unit of work, as it travels through a broker.
///
/// The field set is the published protocol's, so a broker can be inspected with
/// the tools of that ecosystem: `id` names the run, `name` the registered task,
/// `args` the argument value, `retries` how many times it has already failed,
/// `eta` when it may first run and `expires` when it is too late to run it.
/// `queue` and `priority` are the routing a broker uses to pick the next one.
///
/// The two fields below `retries` are how a chain and a chord travel: a message
/// carries the rest of its own chain with it, so no coordinator is needed to
/// advance it.
#derive(Eq, Debug)
pub(all) struct Message {
  id : String
  name : String
  args : Json
  queue : String
  priority : Int32
  /// The earliest time the message may run, in milliseconds since the epoch.
  eta : Int64?
  /// The time after which the message must not run even if it was never
  /// claimed, in milliseconds since the epoch.
  expires : Int64?
  retries : Int32
  /// The rest of the chain, in order. The worker sends the head after this task
  /// succeeds and drops the tail.
  chain : Array[Message]
  /// Set when this message is one part of a chord.
  chord : Chord?
}

///|
pub extend Message with Eq::{not_equal, equal}

///|
pub extend Message with Debug::{to_repr}

///|
/// One part's view of the chord it belongs to: which key counts the parts, which
/// part this is, every part's id so the callback can collect their results, and
/// the message to send once the last part is done.
///
/// Every part carries the same `ids`, so the worker that happens to finish last
/// can collect the whole set without asking a coordinator.
#derive(Eq, Debug)
pub(all) struct Chord {
  key : String
  index : Int32
  total : Int32
  ids : Array[String]
  callback : Message
}

///|
pub extend Chord with Eq::{not_equal, equal}

///|
pub extend Chord with Debug::{to_repr}

///|
/// A message for `name` with argument `args`, queued on `default` with no delay
/// and no retries behind it.
pub fn Message::new(id : String, name : String, args : Json) -> Message {
  {
    id,
    name,
    args,
    queue: "default",
    priority: 0,
    eta: None,
    expires: None,
    retries: 0,
    chain: [],
    chord: None,
  }
}

///|
/// Whether the message is past the time it may still run at `now`.
pub fn Message::is_expired(self : Message, now : Int64) -> Bool {
  match self.expires {
    Some(expires) => now > expires
    None => false
  }
}

///|
/// Whether the message may run at `now`: its `eta`, when set, has come.
pub fn Message::is_due(self : Message, now : Int64) -> Bool {
  match self.eta {
    Some(eta) => now >= eta
    None => true
  }
}
