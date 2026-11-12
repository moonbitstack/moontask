// The SQL store against a real SQLite: an outcome written and read back, a
// flow's runs kept, and the whole path a worker takes to write them.
//
// The database is in memory, so there is nothing to start and nothing to leave
// behind; what it proves is that the statements the store writes are the ones
// SQLite accepts, not the ones a script says it was asked for.

///|
/// The pair of tables these tests use, named apart from the defaults so a
/// database that already holds a queue is not written into.
fn live_tables() -> @sql.Tables {
  { outcome: "mt_live_outcome", run: "mt_live_run", }
}

///|
async test "an outcome is written, replaced and read back from a real database" {
  let driver = @sqlite.SqliteDriver::open(":memory:")
  let store = @sql.Store::inline(
    driver,
    clock=fn() { 1L },
    tables=live_tables(),
  )
  store.ensure()
  // A second ensure must not fail: it is what every start-up calls.
  store.ensure()
  assert_eq(store.dialect(), @moondb.Sqlite)

  let settled : @spec.Outcome = {
    state: @spec.Succeeded,
    result: Some(Json::number(42.0)),
    error: None,
    retries: 0,
    at: 1_700_000_000_000L,
  }
  store.store("live-outcome-1", settled)
  let read = store.fetch("live-outcome-1").unwrap()
  assert_eq(read.state, @spec.Succeeded)
  assert_eq(read.result.unwrap().stringify(), "42")
  assert_eq(read.error is None, true)
  assert_eq(read.at, 1_700_000_000_000L)

  // The same id, settled differently: one row, not two.
  store.store("live-outcome-1", {
    ..settled,
    state: @spec.Failed,
    result: None,
    error: Some("boom"),
  })
  let replaced = store.fetch("live-outcome-1").unwrap()
  assert_eq(replaced.state, @spec.Failed)
  assert_eq(replaced.error, Some("boom"))
  assert_eq(replaced.result is None, true)
  assert_eq(store.fetch("live-outcome-nobody-wrote") is None, true)

  // The row count the database itself reports, not the one the store believes.
  let rows = driver.query("SELECT COUNT(*) FROM mt_live_outcome", [])
  assert_eq(rows[0].int(0), 1)
  driver.close()
}

///|
async test "a flow's runs are kept, newest first, with what each step answered" {
  let driver = @sqlite.SqliteDriver::open(":memory:")
  let store = @sql.Store::inline(
    driver,
    clock=fn() { 1L },
    tables=live_tables(),
  )
  store.ensure()
  let clock = fn() { 1_000_000L }
  let broker = @moontask.MemoryBroker::new(clock~)
  let app = @moontask.App::new(broker, backend=broker, counter=broker, clock~)
  let double = app.task("double", fn(value : Int32, _) { value * 2 })
  let flow = @flow.Flow::new(
    "live-flow",
    schedule=@moontask.Schedule::Every(interval=1000L),
  )
  flow.step("double", double)
  flow.record(store)
  let worker = app.worker()
  let runs = @async.with_task_group(async fn(group) {
    group.spawn_bg(async fn() { worker.run() }, no_wait=true)
    let first = flow.run(app, 2000L, input=Json::number(5))
    let second = flow.run(app, 3000L, input=Json::number(7))
    worker.stop()
    [first, second]
  })
  assert_true(runs[0].ok())
  assert_true(runs[1].ok())

  let kept = store.runs("live-flow", limit=10)
  assert_eq(kept.length(), 2)
  // Newest first: the later instant is the one a reader sees at the top.
  assert_eq(kept[0].at, 3000L)
  assert_eq(kept[0].output_of("double").unwrap().stringify(), "14")
  assert_eq(kept[1].at, 2000L)
  assert_eq(kept[1].output_of("double").unwrap().stringify(), "10")
  driver.close()
}

///|
async test "a result too large for a small column is kept whole" {
  let driver = @sqlite.SqliteDriver::open(":memory:")
  let store = @sql.Store::inline(
    driver,
    clock=fn() { 1L },
    tables=live_tables(),
  )
  store.ensure()
  let mut text = ""
  for _ in 0..<2000 {
    text = text + "0123456789"
  }
  let settled : @spec.Outcome = {
    state: @spec.Succeeded,
    result: Some(Json::string(text)),
    error: None,
    retries: 0,
    at: 1L,
  }
  store.store("big", settled)
  let read = store.fetch("big").unwrap()
  assert_eq(read.result.unwrap().stringify().length(), text.length() + 2)
  driver.close()
}
