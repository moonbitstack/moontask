// A flow is a graph of tasks: the order comes from the steps' dependencies, the
// work happens on the workers, and every run is stamped with its instant.

///|
async test "a step waits for the steps it names, and reads their answers" {
  let manual = Manual::new(1_000_000L)
  let (app, _) = app_on(fn() { manual.now() })
  let order = []
  let extract = app.task("extract", fn(_ : Unit, _) -> Json {
    order.push("extract")
    { "rows": 3 }
  })
  let load = app.task("load", fn(args : Json, _) -> Json {
    order.push("load")
    { "seen": args }
  })
  let flow = @flow.Flow::new("etl")
  flow.step("extract", extract)
  flow.step("load", load, deps=["extract"])
  let run = with_worker(app.worker(), async fn() { flow.run(app, 1000L) })
  assert_true(run.ok())
  assert_eq(order, ["extract", "load"])
  assert_eq(run.output_of("extract").unwrap().stringify(), "{\"rows\":3}")
  assert_eq(
    run.output_of("load").unwrap().stringify(),
    "{\"seen\":{\"extract\":{\"rows\":3}}}",
  )
}

///|
async test "a step is called with its own dependencies, not with everything that has answered" {
  let manual = Manual::new(1_000_000L)
  let (app, _) = app_on(fn() { manual.now() })
  let seen = []
  let left = app.task("left", fn(_ : Unit, _) -> Int32 { 1 })
  let right = app.task("right", fn(_ : Unit, _) -> Int32 { 2 })
  let narrow = app.task("narrow", fn(args : Json, _) -> Int32 {
    seen.push(args.stringify())
    match args {
      Object(fields) => fields.length()
      _ => 0
    }
  })
  let flow = @flow.Flow::new("narrow")
  flow.step("left", left)
  flow.step("right", right)
  // `right` has answered by the time this runs, but it was not named.
  flow.step("narrow", narrow, deps=["left"])
  let run = with_worker(app.worker(), async fn() { flow.run(app, 1000L) })
  assert_true(run.ok())
  assert_eq(seen, ["{\"left\":1}"])
}

///|
async test "steps that wait for nothing run before the step that joins them" {
  let manual = Manual::new(1_000_000L)
  let (app, _) = app_on(fn() { manual.now() })
  let order = []
  let left = app.task("left", fn(_ : Unit, _) -> Int32 {
    order.push("left")
    1
  })
  let right = app.task("right", fn(_ : Unit, _) -> Int32 {
    order.push("right")
    2
  })
  let join = app.task("join", fn(args : Json, _) -> Int32 {
    order.push("join")
    match args {
      Object(fields) => fields.length()
      _ => 0
    }
  })
  let flow = @flow.Flow::new("diamond")
  flow.step("left", left)
  flow.step("right", right)
  flow.step("join", join, deps=["left", "right"])
  let run = with_worker(app.worker(), async fn() { flow.run(app, 1000L) })
  assert_true(run.ok())
  assert_eq(order.length(), 3)
  assert_eq(order[2], "join")
  assert_eq(run.output_of("join").unwrap().stringify(), "2")
}

///|
async test "a step that fails stops the run and the steps behind it" {
  let manual = Manual::new(1_000_000L)
  let (app, _) = app_on(fn() { manual.now() })
  let order = []
  let okay = app.task("okay", fn(_ : Unit, _) -> Int32 {
    order.push("okay")
    1
  })
  let broken = app.task("broken", fn(_ : Json, _) -> Int32 raise {
    fail("the step cannot run")
  })
  let after = app.task("after", fn(_ : Json, _) -> Int32 {
    order.push("after")
    3
  })
  let flow = @flow.Flow::new("broken")
  flow.step("okay", okay)
  flow.step("broken", broken, deps=["okay"])
  flow.step("after", after, deps=["broken"])
  let run = with_worker(app.worker(), async fn() { flow.run(app, 1000L) })
  assert_false(run.ok())
  assert_eq(order, ["okay"])
  match run.status {
    @flow.Failed(got~) => assert_true(got.contains("the step cannot run"))
    @flow.Succeeded => fail("the run reports a step that failed as a success")
  }
}

///|
test "a flow refuses two steps with one id" {
  let manual = Manual::new(1_000_000L)
  let (app, _) = app_on(fn() { manual.now() })
  let task = app.task("task", fn(_ : Unit, _) -> Unit { () })
  let flow = @flow.Flow::new("twice")
  flow.step("a", task)
  let refused = try {
    flow.step("a", task)
    ""
  } catch {
    @flow.Refused::Duplicate(id~) => id
    @flow.Refused::Dangling(node~, dep~) => node + "->" + dep
    @flow.Refused::Empty => "empty"
    @flow.Refused::Missing(kind~) => "missing " + kind
  }
  assert_eq(refused, "a")
}

///|
test "a flow refuses a dependency that is registered after it" {
  let manual = Manual::new(1_000_000L)
  let (app, _) = app_on(fn() { manual.now() })
  let task = app.task("task", fn(_ : Unit, _) -> Unit { () })
  let flow = @flow.Flow::new("forward")
  let refused = try {
    flow.step("a", task, deps=["b"])
    ""
  } catch {
    @flow.Refused::Dangling(node~, dep~) => node + "->" + dep
    @flow.Refused::Duplicate(id~) => "duplicate " + id
    @flow.Refused::Empty => "empty"
    @flow.Refused::Missing(kind~) => "missing " + kind
  }
  assert_eq(refused, "a->b")
}

///|
async test "a backfill runs one run per instant of the schedule" {
  let manual = Manual::new(1_000_000L)
  let (app, _) = app_on(fn() { manual.now() })
  let tick = app.task("tick", fn(_ : Unit, _) -> Int32 { 1 })
  let flow = @flow.Flow::new(
    "hourly",
    schedule=@moontask.Schedule::Every(interval=3600_000L),
  )
  flow.step("tick", tick)
  let runs = with_worker(app.worker(), async fn() {
    flow.backfill(app, start=0L, end=3L * 3600_000L)
  })
  assert_eq(runs.length(), 3)
  assert_eq(runs.map(run => run.at), [0L, 3600_000L, 7200_000L])
  assert_eq(runs[0].thread, "hourly:0")
}

///|
async test "a late run catches up the instants it skipped" {
  let manual = Manual::new(1_000_000L)
  let (app, _) = app_on(fn() { manual.now() })
  let count = []
  let tick = app.task("tick", fn(_ : Unit, _) -> Int32 {
    count.push(1)
    1
  })
  let flow = @flow.Flow::new(
    "catchup",
    schedule=@moontask.Schedule::Every(interval=1000L),
    catchup=true,
  )
  flow.step("tick", tick)
  let runs = with_worker(app.worker(), async fn() {
    let first = flow.run(app, 1000L)
    let caught = flow.catchup(app, now=4000L)
    [first] + caught
  })
  // The run at 1000 came first; catching up to 4000 fills 2000 and 3000.
  assert_eq(runs.map(run => run.at), [1000L, 2000L, 3000L])
  assert_eq(count.length(), 3)
}

///|
async test "a recorder is told every run, failed ones included" {
  let manual = Manual::new(1_000_000L)
  let (app, _) = app_on(fn() { manual.now() })
  let recorder = Recording::new()
  let task = app.task("task", fn(_ : Unit, _) -> Unit { () })
  let flow = @flow.Flow::new(
    "recorded",
    schedule=@moontask.Schedule::Every(interval=1000L),
  )
  flow.record(recorder)
  flow.step("only", task)
  with_worker(app.worker(), async fn() {
    let _ = flow.backfill(app, start=0L, end=2000L)
  })
  let runs = recorder.runs("recorded", limit=10)
  assert_eq(runs.length(), 2)
  assert_eq(runs[0].at, 1000L)
  assert_true(runs[1].ok())
}

///|
async test "a run that stopped at a step runs again from its checkpoint" {
  let manual = Manual::new(1_000_000L)
  let (app, _) = app_on(fn() { manual.now() })
  let attempts = []
  let flaky = app.task("flaky", fn(_ : Unit, _) -> Int32 raise {
    attempts.push(1)
    if attempts.length() == 1 {
      raise Failure::Failure("first time")
    }
    7
  })
  let flow = @flow.Flow::new("resumable")
  flow.keep(@moonstate.MemorySaver::new())
  flow.step("flaky", flaky)
  with_worker(app.worker(), async fn() {
    let first = flow.run(app, 1000L)
    assert_false(first.ok())
    let second = flow.run(app, 1000L)
    assert_true(second.ok())
    assert_eq(second.output_of("flaky").unwrap().stringify(), "7")
  })
}

///|
async test "a flow with no schedule refuses to walk one" {
  let manual = Manual::new(1_000_000L)
  let (app, _) = app_on(fn() { manual.now() })
  let task = app.task("task", fn(_ : Unit, _) -> Unit { () })
  let flow = @flow.Flow::new("unscheduled")
  flow.step("only", task)
  let refused = try {
    let _ = flow.backfill(app, start=0L, end=1000L)
    "none"
  } catch {
    @flow.Refused::Missing(kind~) => kind
    @flow.Refused::Empty => "empty"
    @flow.Refused::Duplicate(id~) => "duplicate " + id
    @flow.Refused::Dangling(node~, dep~) => node + "->" + dep
    other => "unexpected: " + other.to_string()
  }
  assert_eq(refused, "schedule")
}

///|
/// A recorder over an array, newest first.
struct Recording {
  kept : Array[@flow.Run]
}

///|
fn Recording::new() -> Recording {
  { kept: [], }
}

///|
pub impl @flow.Recorder for Recording with fn record(self, run) {
  self.kept.push(run)
}

///|
pub impl @flow.Recorder for Recording with fn runs(self, flow, limit~) {
  let out = []
  for index = self.kept.length() - 1; index >= 0; index = index - 1 {
    if out.length() >= limit {
      break
    }
    if self.kept[index].flow == flow {
      out.push(self.kept[index])
    }
  }
  out
}

///|
pub extend Recording with @flow.Recorder::{record, runs}
