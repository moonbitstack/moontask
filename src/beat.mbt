// The beat: the process that sends periodic tasks.

///|
/// When a beat sends a task.
#derive(Eq, Debug)
pub(all) enum Schedule {
  /// Every `interval` milliseconds, measured from the last fire.
  Every(interval~ : Int64)
  /// On the boundaries a cron expression names.
  Cron(expr~ : @cron.Cron)
  /// Once, at `at`, in milliseconds since the epoch. A time already past fires
  /// on the first tick instead of never.
  At(at~ : Int64)
}

///|
pub extend Schedule with Eq::{not_equal, equal}

///|
pub extend Schedule with Debug::{to_repr}

///|
/// The first time this schedule fires at or after `now`.
pub fn Schedule::first(self : Schedule, now : Int64) -> Int64 {
  match self {
    Every(interval~) => now + interval
    At(at~) => if at > now { at } else { now }
    Cron(expr~) =>
      match cron_next(expr, now) {
        Some(at) => at
        // An expression that will never match again is treated as one that has
        // already fired; the beat drops the entry rather than spinning on it.
        None => now
      }
  }
}

///|
/// The first time this schedule fires strictly after `at`, or `None` when it
/// never fires again.
pub fn Schedule::after(self : Schedule, at : Int64) -> Int64? {
  match self {
    Every(interval~) => Some(at + interval)
    At(_) => None
    Cron(expr~) => cron_next(expr, at)
  }
}

///|
/// The next boundary of `expr` after `at`, in milliseconds.
fn cron_next(expr : @cron.Cron, at : Int64) -> Int64? {
  let moment = moment_of(at)
  match moment {
    Some(moment) =>
      match expr.next(moment) {
        Some(next) => milliseconds_of(next)
        None => None
      }
    None => None
  }
}

///|
/// An instant from milliseconds since the epoch.
pub fn moment_of(at : Int64) -> @moondate.Moment? {
  let seconds = if at >= 0L { at / 1000L } else { (at - 999L) / 1000L }
  let nano = ((at - seconds * 1000L) * 1000000L).to_int()
  Some(@moondate.Moment::of_epoch(seconds, nano~, zone=@moondate.Utc))
}

///|
/// Milliseconds since the epoch of an instant.
pub fn milliseconds_of(moment : @moondate.Moment) -> Int64? {
  match moment.timestamp() {
    Some(seconds) => Some((seconds * 1000.0).round().to_int64())
    None => None
  }
}

///|
/// One thing the beat sends.
struct BeatEntry {
  name : String
  message : @spec.Message
  schedule : Schedule
  mut next : Int64?
}

///|
/// The object that sends tasks on a schedule.
///
/// A beat holds a list of entries and, every tick, sends the ones whose time has
/// come. It keeps the phase: a schedule of every hour fires at the same minute
/// every hour even if one tick ran late, because the next time is computed from
/// the time that was due, not from the time the tick happened.
pub struct Beat {
  app : App
  entries : Array[BeatEntry]
  mut stopping : Bool
}

///|
/// A beat on `app` with nothing scheduled.
pub fn App::beat(self : App) -> Beat {
  { app: self, entries: [], stopping: false, }
}

///|
/// Schedule `task` with `args` under `name`.
///
/// The name identifies the entry, not the task: the same task may be scheduled
/// twice under two names and two schedules.
pub fn[A : ToJson, R] Beat::add(
  self : Beat,
  name : String,
  task : Task[A, R],
  args : A,
  schedule : Schedule,
) -> Unit {
  let message = task.compose(args)
  let next = Some(schedule.first(self.app.now()))
  self.entries.push({ name, message, schedule, next, })
}

///|
/// The entry names and when each fires next, soonest first.
pub fn Beat::pending(self : Beat) -> Array[(String, Int64)] {
  let pending = []
  for entry in self.entries {
    match entry.next {
      Some(at) => pending.push((entry.name, at))
      None => ()
    }
  }
  pending.sort_by(fn(left, right) { left.1.compare(right.1) })
  pending
}

///|
/// Stop after the tick in progress.
pub fn Beat::stop(self : Beat) -> Unit {
  self.stopping = true
}

///|
/// Send due entries, then wait for the next one. Runs until `stop`.
pub async fn Beat::run(self : Beat) -> Unit raise @spec.Refused {
  while !self.stopping {
    let now = self.app.now()
    let due = []
    let mut next_at = now + 50L
    for entry in self.entries {
      match entry.next {
        Some(at) =>
          if at <= now {
            due.push(entry)
          } else if at < next_at {
            next_at = at
          }
        None => ()
      }
    }
    for entry in due {
      if !entry.message.is_expired(now) {
        self.app.broker.publish(entry.message.queue, { ..entry.message, })
      }
      entry.next = entry.schedule.after(entry.next.unwrap_or(now))
    }
    let wait = if next_at <= now { 1L } else { next_at - now }
    @wait.nap(if wait > 50L { 50 } else { wait.to_int() })
  }
}
