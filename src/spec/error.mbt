// Why a call was refused.

///|
/// The one error type of this library.
///
/// A broker, a result store and a chord counter all refuse in these terms, so a
/// caller catches one type no matter which seam failed. `got`/`want`/`limit` are
/// the organisation's words for "what arrived", "what was required" and "the
/// ceiling that was hit".
#derive(Eq, Debug)
pub(all) suberror Refused {
  /// No task, queue or key goes by this name.
  Unknown(name~ : String)
  /// A broker handed over a message that does not decode.
  Malformed(got~ : String)
  /// The call needs a capability the app was not given: a result store, a
  /// chord counter, a worker loop that is not running.
  Missing(kind~ : String)
  /// A wait ran past its limit without the state it was waiting for.
  Timeout(limit~ : Int64)
  /// The run ended in failure; `got` is what the body raised, `retries` how many
  /// attempts it took.
  Failed(got~ : String, retries~ : Int32)
  /// A canvas was built from nothing, or a handle names no run.
  Empty(kind~ : String)
}

///|
pub extend Refused with Eq::{not_equal, equal}

///|
pub extend Refused with Debug::{to_repr}

///|
pub impl Show for Refused with fn output(self, logger) {
  match self {
    Unknown(name~) => logger.write_string("unknown: " + name)
    Malformed(got~) => logger.write_string("malformed: " + got)
    Missing(kind~) => logger.write_string("missing: " + kind)
    Timeout(limit~) => logger.write_string("timed out after \{limit} ms")
    Failed(got~, retries~) =>
      logger.write_string("failed after \{retries} retries: " + got)
    Empty(kind~) => logger.write_string("empty: " + kind)
  }
}

///|
pub extend Refused with Show::{to_string, output}
