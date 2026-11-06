// How a task behaves when the queue is under pressure: how many times it is
// tried, how long between tries, how long one run may take, who confirms it and
// how fast it may go.

///|
/// How long to wait before the next attempt.
///
/// `Fixed` waits the same time every time. `Exponential` doubles from `base`
/// until it reaches `cap`, and with `jitter` on it spreads the wait over the
/// whole interval instead of always waiting the full one — that is what keeps a
/// thousand workers from retrying in the same millisecond after an outage.
///
/// The spread is derived from the task id and the attempt number rather than
/// drawn at random, so the same task retried twice on two machines waits the
/// same both times, and a test can assert the delay.
#derive(Eq, Debug)
pub(all) enum Backoff {
  Fixed(delay~ : Int64)
  Exponential(base~ : Int64, cap~ : Int64, jitter~ : Bool)
}

///|
pub extend Backoff with Eq::{not_equal, equal}

///|
pub extend Backoff with Debug::{to_repr}

///|
/// The wait before attempt `attempt`, counting the first attempt as one.
///
/// `seed` is what the spread is derived from: the task id in practice. Attempt
/// zero never waits, since nothing has failed yet.
pub fn Backoff::delay(self : Backoff, attempt : Int32, seed : String) -> Int64 {
  if attempt <= 0 {
    return 0L
  }
  match self {
    Fixed(delay~) => delay
    Exponential(base~, cap~, jitter~) => {
      let mut wait = base
      let mut step = 1
      while step < attempt && wait < cap {
        wait = wait * 2L
        step = step + 1
      }
      if wait > cap {
        wait = cap
      }
      if jitter {
        // Full spread: the wait lands anywhere in `1..wait`, chosen by a hash of
        // the seed and the attempt so it is reproducible.
        let spread = spread_of(seed, attempt)
        let scaled = (wait.to_double() * spread).to_int64()
        if scaled < 1L {
          1L
        } else {
          scaled
        }
      } else {
        wait
      }
    }
  }
}

///|
/// A number in `0..1` derived from the seed and the attempt: FNV-1a over the
/// two, scaled to the low 16 bits.
fn spread_of(seed : String, attempt : Int32) -> Double {
  let mut hash : UInt64 = 0xCBF29CE484222325UL
  for character in seed {
    hash = (hash ^ character.to_int().to_uint64()) * 0x100000001B3UL
  }
  hash = (hash ^ attempt.to_uint64()) * 0x100000001B3UL
  (hash % 65536UL).to_double() / 65535.0
}

///|
/// How fast one task may go: `count` runs per `per` milliseconds.
#derive(Eq, Debug)
pub(all) struct Rate {
  count : Int32
  per : Int64
}

///|
pub extend Rate with Eq::{not_equal, equal}

///|
pub extend Rate with Debug::{to_repr}

///|
/// Everything about how a task is run, retried and confirmed.
///
/// The defaults are the ones a queue is usually run with, and each of them is
/// the safe side of the choice: no retries (a retry repeats the side effects of
/// the first attempt), no timeout (a hung task is visible in the queue rather
/// than silently cut), confirm on receipt (the message is not redelivered if the
/// worker dies, which is the cheaper mistake for idempotent work).
#derive(Eq, Debug)
pub(all) struct Policy {
  /// The queue a task goes to unless the sender names another. `default`.
  queue : String
  /// How many times a failed run is tried again. 0, so a task that fails is
  /// failed; a caller that wants retries says so.
  retries : Int32
  /// The wait between attempts. `Exponential(1000, 600000, true)`.
  backoff : Backoff
  /// How long one run may take, in milliseconds. `None` waits as long as the
  /// body takes.
  timeout : Int64?
  /// Whether the message is confirmed after the run instead of on receipt.
  /// False, which is at-most-once: a worker killed mid-run does not repeat the
  /// work.
  acks_late : Bool
  /// Whether the result is kept. False, so a task nobody waits on costs nothing.
  ignore_result : Bool
  /// How fast the task may run, per worker. `None` is unlimited.
  rate : Rate?
}

///|
pub extend Policy with Eq::{not_equal, equal}

///|
pub extend Policy with Debug::{to_repr}

///|
/// The defaults, documented field by field on [`Policy`].
pub let policy : Policy = {
  queue: "default",
  retries: 0,
  backoff: Backoff::Exponential(base=1000L, cap=600000L, jitter=true),
  timeout: None,
  acks_late: false,
  ignore_result: false,
  rate: None,
}

///|
/// The wait before attempt `attempt` of a task whose id is `seed`.
pub fn Policy::backoff_delay(
  self : Policy,
  attempt : Int32,
  seed : String,
) -> Int64 {
  self.backoff.delay(attempt, seed)
}
