// The store and the run records against a driver that answers from a script:
// what statements are issued, in which dialect, and how a row is read back.

///|
/// A driver that keeps what it was asked and answers with the rows a test put in
/// front of it. The SQL a store writes is a property of the store, not of a
/// server, so this is where the spelling is checked; the round trip against a
/// real server is the integration module's job.
struct Sketch {
  dialect : @moondb.Dialect
  issued : Array[(String, Array[@moondb.Value])]
  mut rows : Array[@moondb.Row]
}

///|
fn Sketch::new(dialect : @moondb.Dialect) -> Sketch {
  { dialect, issued: [], rows: [], }
}

///|
fn Sketch::sql(self : Sketch, index : Int32) -> String {
  self.issued[index].0
}

///|
fn Sketch::params(self : Sketch, index : Int32) -> Array[@moondb.Value] {
  self.issued[index].1
}

///|
pub impl @moondb.AsyncDriver for Sketch with fn execute(self, sql, params) -> @moondb.ExecResult raise {
  self.issued.push((sql, params))
  { rows_affected: 1L, last_insert_id: 0L, }
}

///|
pub impl @moondb.AsyncDriver for Sketch with fn query(self, sql, params) -> Array[
  @moondb.Row,
] raise {
  self.issued.push((sql, params))
  self.rows
}

///|
pub impl @moondb.AsyncDriver for Sketch with fn begin(_self) -> Unit raise {

}

///|
pub impl @moondb.AsyncDriver for Sketch with fn commit(_self) -> Unit raise {

}

///|
pub impl @moondb.AsyncDriver for Sketch with fn rollback(_self) -> Unit raise {

}

///|
pub impl @moondb.AsyncDriver for Sketch with fn close(_self) -> Unit {

}

///|
pub impl @moondb.AsyncDriver for Sketch with fn dialect(self) -> @moondb.Dialect {
  self.dialect
}

///|
/// A row of the outcome table, in the order the store reads it back.
fn outcome_row(
  state : String,
  result : @moondb.Value,
  error : @moondb.Value,
  retries : Int32,
  at : Int64,
) -> @moondb.Row {
  {
    columns: ["state", "result", "error", "retries", "updated_at"],
    values: [
      @moondb.Value::Text(state),
      result,
      error,
      @moondb.Value::Int(retries),
      @moondb.Value::Int64(at),
    ],
  }
}

///|
/// A row of the run table, in the order the store reads it back.
fn run_row(
  thread : String,
  at : Int64,
  status : String,
  error : @moondb.Value,
  state : @moondb.Value,
) -> @moondb.Row {
  {
    columns: ["thread", "instant", "status", "error", "state"],
    values: [
      @moondb.Value::Text(thread),
      @moondb.Value::Int64(at),
      @moondb.Value::Text(status),
      error,
      state,
    ],
  }
}

///|
async test "an outcome is written as one upsert, in the dialect's spelling" {
  let sketch = Sketch::new(@moondb.Sqlite)
  let store = @sql.Store::new(sketch, clock=fn() { 0L })
  let outcome : @spec.Outcome = {
    state: @spec.Succeeded,
    result: Some(Json::number(42.0)),
    error: None,
    retries: 1,
    at: 1700L,
  }
  store.store("run-1", outcome)
  assert_eq(store.dialect(), @moondb.Sqlite)
  let sql = sketch.sql(0)
  assert_true(sql.contains("INSERT INTO moontask_outcome"))
  assert_true(sql.contains("ON CONFLICT (id) DO UPDATE"))
  let params = sketch.params(0)
  assert_eq(params, [
    @moondb.Value::Text("run-1"),
    @moondb.Value::Text("SUCCESS"),
    @moondb.Value::Text("42"),
    @moondb.Value::Null,
    @moondb.Value::Int(1),
    @moondb.Value::Int64(1700L),
  ])
}

///|
async test "postgres numbers its placeholders and mysql has a clause of its own" {
  let postgres = Sketch::new(@moondb.Postgres)
  let store = @sql.Store::new(postgres, clock=fn() { 0L })
  let outcome : @spec.Outcome = {
    state: @spec.Pending,
    result: None,
    error: None,
    retries: 0,
    at: 0L,
  }
  store.store("a", outcome)
  assert_true(postgres.sql(0).contains("$1, $2, $3, $4, $5, $6"))
  assert_true(postgres.sql(0).contains("excluded.state"))

  let mysql = Sketch::new(@moondb.Mysql)
  let other = @sql.Store::new(mysql, clock=fn() { 0L })
  other.store("a", outcome)
  assert_true(mysql.sql(0).contains("ON DUPLICATE KEY UPDATE"))
  assert_true(mysql.sql(0).contains("state = VALUES(state)"))
  assert_true(mysql.sql(0).contains("VALUES (?, ?, ?, ?, ?, ?)"))
}

///|
async test "a row is read back into the outcome it was written from" {
  let sketch = Sketch::new(@moondb.Sqlite)
  sketch.rows = [
    outcome_row(
      "SUCCESS",
      @moondb.Value::Text("42"),
      @moondb.Value::Null,
      2,
      1700L,
    ),
  ]
  let store = @sql.Store::new(sketch, clock=fn() { 0L })
  let outcome = store.fetch("run-1").unwrap()
  assert_eq(outcome.state, @spec.Succeeded)
  assert_eq(outcome.result.unwrap().stringify(), "42")
  assert_eq(outcome.error is None, true)
  assert_eq(outcome.retries, 2)
  assert_eq(outcome.at, 1700L)
  assert_true(sketch.sql(0).contains("WHERE id = ?"))
  assert_eq(sketch.params(0), [@moondb.Value::Text("run-1")])
}

///|
async test "a run the store has never heard of answers nothing" {
  let sketch = Sketch::new(@moondb.Sqlite)
  let store = @sql.Store::new(sketch, clock=fn() { 0L })
  assert_eq(store.fetch("missing") is None, true)
}

///|
async test "an unknown state label is refused rather than guessed" {
  let sketch = Sketch::new(@moondb.Sqlite)
  sketch.rows = [
    outcome_row("WEIRD", @moondb.Value::Null, @moondb.Value::Null, 0, 0L),
  ]
  let store = @sql.Store::new(sketch, clock=fn() { 0L })
  let words = try {
    let _ = store.fetch("run-1")
    "read it"
  } catch {
    @spec.Refused::Unknown(name~) => name
    @spec.Refused::Malformed(got~) => got
    @spec.Refused::Transport(got~) => got
    @spec.Refused::Missing(kind~) => kind
    @spec.Refused::Timeout(limit~) => limit.to_string()
    @spec.Refused::Failed(got~, ..) => got
    @spec.Refused::Empty(kind~) => kind
  }
  assert_eq(words, "WEIRD")
}

///|
async test "ensure creates both tables and nothing else" {
  let sketch = Sketch::new(@moondb.Mysql)
  let store = @sql.Store::new(sketch, clock=fn() { 0L })
  store.ensure()
  assert_eq(sketch.issued.length(), 2)
  assert_true(
    sketch.sql(0).contains("CREATE TABLE IF NOT EXISTS moontask_outcome"),
  )
  assert_true(sketch.sql(0).contains("LONGTEXT"))
  assert_true(sketch.sql(0).contains("VARCHAR(191)"))
  assert_true(sketch.sql(1).contains("CREATE TABLE IF NOT EXISTS moontask_run"))
  assert_true(sketch.sql(1).contains("PRIMARY KEY (flow, thread)"))
}

///|
async test "a run is written with its instant, its status and the time it was kept" {
  let sketch = Sketch::new(@moondb.Sqlite)
  let store = @sql.Store::new(sketch, clock=fn() { 9999L })
  let run : @flow.Run = {
    flow: "etl",
    thread: "etl:1000",
    at: 1000L,
    state: { "extract": 3 },
    status: @flow.Succeeded,
  }
  store.record(run)
  assert_true(sketch.sql(0).contains("INSERT INTO moontask_run"))
  let params = sketch.params(0)
  assert_eq(params, [
    @moondb.Value::Text("etl"),
    @moondb.Value::Text("etl:1000"),
    @moondb.Value::Int64(1000L),
    @moondb.Value::Text("SUCCESS"),
    @moondb.Value::Null,
    @moondb.Value::Text("{\"extract\":3}"),
    @moondb.Value::Int64(9999L),
  ])
}

///|
async test "runs reads a failure back with the words it failed with" {
  let sketch = Sketch::new(@moondb.Sqlite)
  sketch.rows = [
    run_row(
      "etl:2000",
      2000L,
      "FAILURE",
      @moondb.Value::Text("step load: boom"),
      @moondb.Value::Text("{\"extract\":3}"),
    ),
    run_row(
      "etl:1000",
      1000L,
      "SUCCESS",
      @moondb.Value::Null,
      @moondb.Value::Text("{\"extract\":3,\"load\":6}"),
    ),
  ]
  let store = @sql.Store::new(sketch, clock=fn() { 0L })
  let runs = store.runs("etl", limit=10)
  assert_eq(runs.length(), 2)
  assert_eq(runs[0].thread, "etl:2000")
  assert_false(runs[0].ok())
  match runs[0].status {
    @flow.Failed(got~) => assert_eq(got, "step load: boom")
    @flow.Succeeded => fail("a failed run reads back as a success")
  }
  assert_eq(runs[1].output_of("load").unwrap().stringify(), "6")
  assert_true(sketch.sql(0).contains("ORDER BY instant DESC"))
  assert_eq(sketch.params(0), [
    @moondb.Value::Text("etl"),
    @moondb.Value::Int(10),
  ])
}
