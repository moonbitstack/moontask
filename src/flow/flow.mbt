// A workflow: steps that are tasks, one run per logical instant, and a backfill
// that walks a schedule.

///|
/// Why a flow cannot be defined or run as asked.
#derive(Eq, Debug)
pub(all) suberror Refused {
  /// Two steps go by the same id.
  Duplicate(id~ : String)
  /// A step waits for one that was not registered before it.
  Dangling(node~ : String, dep~ : String)
  /// A flow with no steps has nothing to run.
  Empty
  /// The call needs something the flow does not have: a schedule to walk, a
  /// previous run to catch up from.
  Missing(kind~ : String)
}

///|
pub extend Refused with Eq::{not_equal, equal}

///|
pub extend Refused with Debug::{to_repr}

///|
/// One step: the task it runs and the steps whose answers it reads.
///
/// A step names only steps registered before it, which is what keeps a flow a
/// directed acyclic graph by construction: an edge can only point backwards.
struct Step {
  id : String
  task : String
  deps : Array[String]
}

///|
/// A workflow.
///
/// The order of the steps is the order they were registered, and the graph is
/// derived from it: a step runs once every step it waits for has answered, and
/// steps that wait for nothing run together. What each step answers is kept in
/// the run's state, keyed by its id, and a step that waits for others is called
/// with an object of their answers — under their ids — as its argument.
///
/// A flow does not run anything itself: `run` hands the steps to an app's
/// workers, and the graph that orders them is a state graph, so a run that stops
/// halfway can be resumed where it stopped.
pub struct Flow {
  id : String
  schedule : @moontask.Schedule?
  catchup : Bool
  steps : Array[Step]
  mut saver : &@moonstate.Saver?
  mut recorder : &Recorder?
  mut last : Int64?
}

///|
/// A flow named `id`.
///
/// `schedule` makes it a periodic flow: `backfill` walks the instants the
/// schedule names, and `catchup` fills the ones a late start skipped. `catchup`
/// is whether `run` fills that gap by itself, which needs to know the previous
/// instant — from this object, or from the recorder when there is one.
pub fn Flow::new(
  id : String,
  schedule? : @moontask.Schedule,
  catchup~ : Bool = false,
) -> Flow {
  { id, schedule, catchup, steps: [], saver: None, recorder: None, last: None, }
}

///|
/// The name the graph and the run records go by.
pub fn Flow::id(self : Flow) -> String {
  self.id
}

///|
/// When this flow is due, or `None` for one that runs only when asked.
pub fn Flow::schedule(self : Flow) -> @moontask.Schedule? {
  self.schedule
}

///|
/// The steps in the order they were registered.
pub fn Flow::step_ids(self : Flow) -> Array[String] {
  self.steps.map(step => step.id)
}

///|
/// What `id` waits for, or `None` when no step goes by that name.
pub fn Flow::deps_of(self : Flow, id : String) -> Array[String]? {
  for step in self.steps {
    if step.id == id {
      return Some(step.deps)
    }
  }
  None
}

///|
/// Keep a checkpoint of the run after every superstep, so a run that stops can
/// be resumed at the step it stopped on. Without one a run is only as durable as
/// the messages in the broker.
pub fn Flow::keep(self : Flow, saver : &@moonstate.Saver) -> Unit {
  self.saver = Some(saver)
}

///|
/// Write every run this flow ends to `recorder`.
pub fn Flow::record(self : Flow, recorder : &Recorder) -> Unit {
  self.recorder = Some(recorder)
}

///|
/// Add `task` as the step `id`, waiting for `deps`.
///
/// The argument each step is called with is the run's input when it waits for
/// nothing, and an object of its dependencies' answers — keyed by their ids —
/// when it does. Both cross the queue as JSON, so a step is a task like any
/// other and runs wherever a worker picks it up.
pub fn[A, R] Flow::step(
  self : Flow,
  id : String,
  task : @moontask.Task[A, R],
  deps~ : Array[String] = [],
) -> Unit raise Refused {
  if id == "" {
    raise Refused::Missing(kind="step id")
  }
  for step in self.steps {
    if step.id == id {
      raise Refused::Duplicate(id~)
    }
  }
  for dep in deps {
    if !self.has(dep) {
      raise Refused::Dangling(node=id, dep~)
    }
  }
  self.steps.push({ id, task: task.name(), deps: deps.copy(), })
}

///|
fn Flow::has(self : Flow, id : String) -> Bool {
  for step in self.steps {
    if step.id == id {
      return true
    }
  }
  false
}
