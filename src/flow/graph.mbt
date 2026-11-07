// The graph a flow compiles to, and the ways a run is started.

///|
/// A run's state before any step has answered: an object with nothing in it.
fn empty_state() -> Json {
  let fields : Map[String, Json] = Map([])
  Json(fields)
}

///|
/// File one step's answer under its id. An update that names no step, or a state
/// that is not an object, leaves the state as it was: a merge never throws away
/// what a run already has.
fn file_output(state : Json, update : Json) -> Json {
  let (id, output) = match update {
    Object(update_fields) =>
      match update_fields.get("step") {
        Some(String(id)) =>
          (id, update_fields.get("output").unwrap_or(Json::null()))
        _ => return state
      }
    _ => return state
  }
  let next : Map[String, Json] = Map([])
  match state {
    Object(fields) =>
      for key, value in fields {
        next[key] = value
      }
    // Before the first step answers, the state is the run's input, which need
    // not be an object. Filing the first answer starts the record of answers.
    _ => ()
  }
  next[id] = output
  Json(next)
}

///|
/// The graph behind this flow: an entry, a node per step, and an edge per
/// dependency.
///
/// The entry writes nothing: it exists so that a flow whose first steps wait for
/// nothing still has one node a run can start at, and so that all of those steps
/// are queued in the same superstep.
///
/// A step's body is never run here. A flow is dispatched, not executed in this
/// process: the body is registered on the workers and the queue carries its
/// argument, so a graph compiled with an executor that runs nodes locally fails
/// at the first step rather than writing nothing.
fn Flow::build(
  self : Flow,
  app : @moontask.App,
  executor : @moontask.Dispatch,
  input : Json,
) -> @moonstate.Graph[Json, Json] raise @moonstate.Refused {
  let mut builder = @moonstate.Builder::new(merge=file_output, initial=input)
  let entry = self.id + "@entry"
  builder = builder.node(entry, (_state, _run) => [])
  for step in self.steps {
    builder = builder.node(step.id, (_state, _run) => {
      let message = "step \{step.id} runs on a worker; run the flow through App::executor"
      raise Failure::Failure(message)
    })
    // The route is how the executor — here or on another machine — finds the
    // body behind a node name, and what the node is called with.
    app.route(self.id, step.id, step.task, deps=step.deps)
  }
  for step in self.steps {
    if step.deps.is_empty() {
      builder = builder.edge(entry, step.id)
    } else {
      for dep in step.deps {
        builder = builder.edge(dep, step.id)
      }
    }
  }
  builder = builder.entry(entry)
  match self.saver {
    Some(saver) => builder.compile(self.id, saver~, executor~)
    None => builder.compile(self.id, executor~)
  }
}

///|
/// What a run's record says about why it stopped.
///
/// A failed node's message is the task's own words, and it is the one thing a
/// reader of a run record needs, so it is lifted out of the graph error rather
/// than left as the name of the variant.
fn reason_of(error : Error) -> String {
  match error {
    @moonstate.GraphError::NodeFailed(node~, message~) =>
      "step " + node + ": " + message
    other => other.to_string()
  }
}

///|
/// Where a run got to when it stopped, from the last checkpoint it kept.
async fn last_state(
  graph : @moonstate.Graph[Json, Json],
  thread : String,
) -> Json {
  let found = Ok(graph.state(thread)) catch { _ => Err(()) }
  match found {
    Ok(Some(snapshot)) => snapshot.state
    _ => empty_state()
  }
}

///|
/// Run one instant, record it, and answer it — a failed run is an answer, not a
/// raise: the status says what happened and the record keeps it.
async fn Flow::attempt(
  self : Flow,
  app : @moontask.App,
  at : Int64,
  input : Json,
) -> Run {
  if self.steps.is_empty() {
    raise Refused::Empty
  }
  let executor = app.executor()
  let graph = self.build(app, executor, input)
  let thread = "\{self.id}:\{at}"
  let outcome = Ok(graph.run(input, thread~)) catch {
    error => Err((error : Error))
  }
  let run = match outcome {
    Ok(state) => { flow: self.id, thread, at, state, status: Succeeded, }
    Err(error) => {
      // Where a run got to is worth keeping: the last checkpoint that finished
      // says which step stopped it.
      let state = match self.saver {
        Some(_) => last_state(graph, thread)
        None => empty_state()
      }
      {
        flow: self.id,
        thread,
        at,
        state,
        status: Failed(got=reason_of(error)),
      }
    }
  }
  match self.recorder {
    Some(recorder) => recorder.record(run)
    None => ()
  }
  run
}

///|
/// Run the flow at the logical instant `at`, waiting for every step to settle.
///
/// `at` is the instant the run belongs to, not the wall clock: a flow rerun for
/// last Monday is stamped last Monday. With a schedule and `catchup` on, the
/// instants between the last known run and `at` are run first.
///
/// One instant is one thread of the graph, so running the same instant twice
/// resumes the first run when it has a saver, and starts again when it has none.
pub async fn Flow::run(
  self : Flow,
  app : @moontask.App,
  at : Int64,
  input? : Json,
) -> Run {
  if self.catchup {
    let _ = self.fill(app, at)
  }
  let run = self.attempt(app, at, input.unwrap_or(Json::null()))
  self.last = Some(at)
  run
}

///|
/// Run every instant the flow's schedule names in `[start, end)`.
///
/// This is how a week of missed runs is made up: each instant is its own run,
/// stamped with the instant it belongs to, and one that fails does not stop the
/// ones behind it. The answer holds the runs that settled; the recorder holds
/// every run, failed ones included.
pub async fn Flow::backfill(
  self : Flow,
  app : @moontask.App,
  start~ : Int64,
  end~ : Int64,
) -> Array[Run] {
  let schedule = match self.schedule {
    Some(schedule) => schedule
    None => raise Refused::Missing(kind="schedule")
  }
  if self.steps.is_empty() {
    raise Refused::Empty
  }
  let runs = []
  for at in schedule.within(start, end) {
    let run = self.attempt(app, at, Json::null())
    if run.ok() {
      runs.push(run)
    }
  }
  runs
}

///|
/// The newest instant this flow is known to have run: the last one this object
/// ran, or the newest the recorder holds.
async fn Flow::known_last(self : Flow) -> Int64? {
  match self.last {
    Some(at) => Some(at)
    None =>
      match self.recorder {
        Some(recorder) =>
          match recorder.runs(self.id, limit=1) {
            [run, ..] => Some(run.at)
            _ => None
          }
        None => None
      }
  }
}

///|
/// Run the instants a late start skipped, up to but not including `at`.
async fn Flow::fill(self : Flow, app : @moontask.App, at : Int64) -> Array[Run] {
  let schedule = match self.schedule {
    Some(schedule) => schedule
    None => return []
  }
  let last = match self.known_last() {
    Some(last) => last
    None => return []
  }
  if last >= at {
    return []
  }
  let window = schedule.within(last, at)
  if window.is_empty() {
    return []
  }
  // The window starts at the instant already run; the rest are the ones missed.
  let runs = []
  for index = 1; index < window.length(); index = index + 1 {
    let run = self.attempt(app, window[index], Json::null())
    if run.ok() {
      runs.push(run)
    }
  }
  runs
}

///|
/// Run the instants that should have run between the last known run and `now`.
///
/// This is the explicit form of what `catchup` does inside `run`, and it refuses
/// when there is no previous instant to start from: a flow that has never run
/// has nothing to catch up.
pub async fn Flow::catchup(
  self : Flow,
  app : @moontask.App,
  now~ : Int64,
) -> Array[Run] {
  if self.schedule is None {
    raise Refused::Missing(kind="schedule")
  }
  if self.known_last() is None {
    raise Refused::Missing(kind="last run")
  }
  self.fill(app, now)
}
