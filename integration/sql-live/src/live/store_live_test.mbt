// The SQL store against a real PostgreSQL: an outcome written and read back, a
// flow's runs kept, and the whole path a worker takes to write them.

///|
/// A connection to the server these tests were pointed at, or `None` when the
/// caller is not asking for a live run.
///
/// With `MOONTASK_SQL_LIVE` set, a server that does not answer is a failure:
/// a suite that quietly skips is a suite that proves nothing on the machine
/// where it matters.
async fn connect() -> @moonpostgres.PgConn? {
  if @env.get_env_var("MOONTASK_SQL_LIVE") is None {
    return None
  }
  let host = @env.get_env_var("MOONTASK_SQL_HOST").unwrap_or("127.0.0.1")
  let port = @env.get_env_var("MOONTASK_SQL_PORT").unwrap_or("5432")
  let user = @env.get_env_var("MOONTASK_SQL_USER").unwrap_or("postgres")
  let password = @env.get_env_var("MOONTASK_SQL_PASSWORD").unwrap_or("postgres")
  let database = @env.get_env_var("MOONTASK_SQL_DATABASE").unwrap_or("test")
  let number = @moonpostgres.parse_int(port).unwrap_or(5432)
  Some(@moonpostgres.PgConn::connect(host, number, user, password, database))
}

///|
/// The pair of tables these tests use. They are named apart from the defaults so
/// a run over a database that already holds a queue does not write into it.
fn live_tables() -> @sql.Tables {
  { outcome: "mt_live_outcome", run: "mt_live_run", }
}

///|
async test "an outcome is written, replaced and read back from a real server" {
  let conn = match connect() {
    Some(conn) => conn
    None => return
  }
  let store = @sql.Store::new(conn, clock=fn() { 1L }, tables=live_tables())
  store.ensure()
  // A second ensure must not fail: it is what every start-up calls.
  store.ensure()
  assert_eq(store.dialect(), @moondb.Postgres)

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
}

///|
async test "a flow's runs are kept, newest first, with what each step answered" {
  let conn = match connect() {
    Some(conn) => conn
    None => return
  }
  let store = @sql.Store::new(conn, clock=fn() { 1L }, tables=live_tables())
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
}
