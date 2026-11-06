// A job: how often it runs, where in the period it lands, and what it does.

///|
/// One rhythm inside a process.
///
/// Built by [`every`] and refined in a chain: `every(10).minutes()` is ten
/// minutes, `.at(":30")` puts it on the half minute, and `.run(action)` gives it
/// something to do. The interval is kept in milliseconds, which is the unit the
/// rest of this library uses; `every(n)` on its own is n milliseconds.
pub struct Job {
  name : String
  /// The length of one period, in milliseconds.
  interval : Int64
  /// Where inside the period the run lands, in milliseconds from the start of
  /// the period.
  anchor : Int64
  action : (async () -> Unit)?
  mut next : Int64
  mut runs : Int32
  mut failures : Int32
}

///|
/// A job every `milliseconds`, before a unit is chosen.
///
/// `every(10).seconds()` is ten seconds and `every(10).minutes()` is ten
/// minutes; `every(250)` on its own is two hundred and fifty milliseconds.
pub fn every(milliseconds : Int64) -> Job {
  let interval = if milliseconds > 0L { milliseconds } else { 1L }
  {
    name: "job",
    interval,
    anchor: 0L,
    action: None,
    next: 0L,
    runs: 0,
    failures: 0,
  }
}

///|
/// The interval read as seconds.
pub fn Job::seconds(self : Job) -> Job {
  { ..self, interval: self.interval * 1000L, }
}

///|
/// The interval read as minutes.
pub fn Job::minutes(self : Job) -> Job {
  { ..self, interval: self.interval * 60000L, }
}

///|
/// The interval read as hours.
pub fn Job::hours(self : Job) -> Job {
  { ..self, interval: self.interval * 3600000L, }
}

///|
/// The interval read as days.
pub fn Job::days(self : Job) -> Job {
  { ..self, interval: self.interval * 86400000L, }
}

///|
/// Where in the period the run lands: `":30"` is thirty seconds past the
/// boundary, `"15:30"` fifteen minutes and thirty seconds past it, and
/// `"9:15:00"` nine hours, fifteen minutes past it.
///
/// A time that does not fit the period is refused rather than rounded, since a
/// rhythm that fires at a time it was not asked for is worse than one that does
/// not start.
pub fn Job::at(self : Job, text : StringView) -> Job raise @spec.Refused {
  let offset = offset_of(text)
  if offset >= self.interval {
    raise @spec.Refused::Malformed(got=text.to_owned())
  }
  { ..self, anchor: offset, }
}

///|
/// A name for the job, for a page that lists what is scheduled.
pub fn Job::named(self : Job, name : String) -> Job {
  { ..self, name, }
}

///|
/// What the job does, and the job itself so the chain can continue.
pub fn Job::run(self : Job, action : async () -> Unit) -> Job {
  { ..self, action: Some(action), }
}

///|
/// The name of the job, `job` until one is given.
pub fn Job::name(self : Job) -> String {
  self.name
}

///|
/// How long one period is, in milliseconds.
pub fn Job::interval(self : Job) -> Int64 {
  self.interval
}

///|
/// When the job runs next, in milliseconds since the epoch.
pub fn Job::next_at(self : Job) -> Int64 {
  self.next
}

///|
/// How many times the job has run.
pub fn Job::runs(self : Job) -> Int32 {
  self.runs
}

///|
/// How many of those runs raised.
pub fn Job::failures(self : Job) -> Int32 {
  self.failures
}

///|
/// `"SS"`, `"MM:SS"` or `"HH:MM:SS"`, with the colon the caller wrote, as
/// milliseconds.
fn offset_of(text : StringView) -> Int64 raise @spec.Refused {
  let parts = text.split(":").collect()
  if parts.length() == 0 || parts.length() > 3 {
    raise @spec.Refused::Malformed(got=text.to_owned())
  }
  let mut offset = 0L
  for part in parts {
    // An empty field is a zero, so ":30" is thirty seconds and "1::30" is one
    // minute and thirty seconds.
    let field = if part.length() == 0 {
      0L
    } else {
      @string.parse_int64(part) catch {
        _ => raise @spec.Refused::Malformed(got=text.to_owned())
      }
    }
    if field < 0L {
      raise @spec.Refused::Malformed(got=text.to_owned())
    }
    // Each field is sixty times the one after it, so reading left to right
    // leaves the last field as seconds.
    offset = offset * 60L + field
  }
  offset * 1000L
}

///|
/// The first time this job fires strictly after `after`.
///
/// The rhythm is anchored to the epoch and shifted by the job's offset, so a job
/// every ten minutes at `":30"` fires at 00:00:30, 00:10:30 and so on, whatever
/// time it was added at. Strictly after is what makes a loop that has just fired
/// ask for the run after it, rather than for the one it has just done.
pub fn Job::next_after(self : Job, after : Int64) -> Int64 {
  let period = self.interval
  let shifted = after - self.anchor
  let mut periods = shifted / period
  if shifted % period < 0L {
    periods = periods - 1L
  }
  let base = periods * period + self.anchor
  if base > after {
    base
  } else {
    base + period
  }
}
