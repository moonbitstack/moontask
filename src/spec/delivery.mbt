// What a delivery is, what a result is, and the label a state writes on the wire.

///|
/// Where a task is in its life. The set is the one a caller has to be able to
/// tell apart, and it is the one published result stores agree on:
///
/// - `Pending` — accepted by a broker, not yet picked up.
/// - `Received` — a worker took it off the queue.
/// - `Started` — the body is running.
/// - `Retrying` — the body failed and the task is queued again.
/// - `Succeeded` — the body returned; the result is stored.
/// - `Failed` — the body raised on its last attempt; the error is stored.
/// - `Revoked` — cancelled before or during the run; it will not be sent again.
#derive(Eq, Debug)
pub(all) enum State {
  Pending
  Received
  Started
  Retrying
  Succeeded
  Failed
  Revoked
}

///|
pub extend State with Eq::{not_equal, equal}

///|
pub extend State with Debug::{to_repr}

///|
/// The label a result store writes for a state. These are the labels the
/// published protocol uses, so a result written by another producer reads back
/// as the same state.
pub fn State::code(self : State) -> String {
  match self {
    Pending => "PENDING"
    Received => "RECEIVED"
    Started => "STARTED"
    Succeeded => "SUCCESS"
    Failed => "FAILURE"
    Retrying => "RETRY"
    Revoked => "REVOKED"
  }
}

///|
/// The state a store's label names. An unknown label is refused rather than
/// read as `Pending`: a store that grew a state this library does not know is a
/// store this library must not guess about.
pub fn State::of(code : StringView) -> State raise Refused {
  match code {
    "PENDING" => Pending
    "RECEIVED" => Received
    "STARTED" => Started
    "SUCCESS" => Succeeded
    "FAILURE" => Failed
    "RETRY" => Retrying
    "REVOKED" => Revoked
    _ => raise Refused::Unknown(name=code.to_owned())
  }
}

///|
pub impl Show for State with fn output(self, logger) {
  logger.write_string(self.code())
}

///|
pub extend State with Show::{to_string, output}

///|
/// Whether no further state will follow. A caller waiting for a result stops
/// here.
pub fn State::is_final(self : State) -> Bool {
  match self {
    Succeeded | Failed | Revoked => true
    Pending | Received | Started | Retrying => false
  }
}

///|
/// What a worker records when a run ends: the state it ended in, the value or
/// the error, and when.
///
/// `result` is the decoded return value of the body and `error` is the message
/// of the raised error; exactly one of them is set for a final state.
#derive(Eq, Debug)
pub(all) struct Outcome {
  state : State
  result : Json?
  error : String?
  retries : Int32
  at : Int64
}

///|
pub extend Outcome with Eq::{not_equal, equal}

///|
pub extend Outcome with Debug::{to_repr}

///|
/// The outcome of a run that returned `value`.
pub fn outcome_succeeded(value : Json, retries : Int32, at : Int64) -> Outcome {
  { state: Succeeded, result: Some(value), error: None, retries, at, }
}

///|
/// The outcome of a run that raised `error` on attempt `retries`.
pub fn outcome_failed(error : String, retries : Int32, at : Int64) -> Outcome {
  { state: Failed, result: None, error: Some(error), retries, at, }
}

///|
/// What a broker hands a worker: one message, the queue it came from, and the
/// receipt the broker needs back to settle it.
///
/// `receipt` is opaque to this library. An in-memory broker puts a sequence
/// there, an ordered-log broker the id of the entry; whatever it is, the worker
/// only ever hands it back to the same broker.
#derive(Eq, Debug)
pub(all) struct Delivery {
  message : Message
  queue : String
  receipt : String
}

///|
pub extend Delivery with Eq::{not_equal, equal}

///|
pub extend Delivery with Debug::{to_repr}

///|
/// What a broker reports about one of its queues.
///
/// `unacked` counts messages a worker holds and has not settled, which is the
/// number a dashboard shows next to the ready count so a stuck consumer is
/// visible.
#derive(Eq, Debug)
pub(all) struct QueueInfo {
  name : String
  ready : Int64
  delayed : Int64
  unacked : Int64
}

///|
pub extend QueueInfo with Eq::{not_equal, equal}

///|
pub extend QueueInfo with Debug::{to_repr}
