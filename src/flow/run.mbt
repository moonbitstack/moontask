// A run: what happened, at which logical instant, and what each step answered.

///|
/// How a run ended.
///
/// A run that fails is also a run that ran: it is recorded with the message it
/// stopped on, because the record is what a backfill or a page reads afterwards.
#derive(Eq, Debug)
pub(all) enum Status {
  Succeeded
  Failed(got~ : String)
}

///|
pub extend Status with Eq::{not_equal, equal}

///|
pub extend Status with Debug::{to_repr}

///|
/// The label a record carries. These are the words a table and a page show, so a
/// run written by one process reads back as the same thing in another.
pub fn Status::code(self : Status) -> String {
  match self {
    Succeeded => "SUCCESS"
    Failed(_) => "FAILURE"
  }
}

///|
/// The status a label names, with the message a failure carries.
pub fn Status::of(code : StringView, got~ : String = "") -> Status {
  match code {
    "SUCCESS" => Succeeded
    _ => Failed(got~)
  }
}

///|
/// One run of a flow.
///
/// `at` is the logical instant the run belongs to, not the time it started: a
/// backfill of last week produces runs stamped last week, which is what makes
/// two runs of the same flow comparable.
pub(all) struct Run {
  flow : String
  thread : String
  at : Int64
  /// The value each step answered, keyed by step id.
  state : Json
  status : Status
}

///|
/// The value `step` answered, or `None` when the run has not reached it.
pub fn Run::output_of(self : Run, step : String) -> Json? {
  match self.state {
    Object(fields) => fields.get(step)
    _ => None
  }
}

///|
/// Whether every step of the run settled.
pub fn Run::ok(self : Run) -> Bool {
  self.status is Succeeded
}

///|
pub impl Show for Run with fn output(self, logger) {
  match self.status {
    Succeeded => logger.write_string("run \{self.thread}: succeeded")
    Failed(got~) => logger.write_string("run \{self.thread}: failed, " + got)
  }
}

///|
pub extend Run with Show::{to_string, output}

///|
/// Where a flow's runs are kept.
///
/// A recorder is told every run as it ends, successful or not, and answers the
/// newest runs of a flow. `runs` answers newest first, because the question it is
/// asked is "what happened last".
///
/// Implementing this is all a store needs to keep a workflow's history: the `sql`
/// module implements it over a table, and a test over an array.
pub(open) trait Recorder {
  async fn record(Self, Run) -> Unit
  async fn runs(Self, flow : String, limit~ : Int32) -> Array[Run]
}
