// The store: outcomes and workflow runs kept in two SQL tables.

///|
/// A result store and a run recorder on one connection.
///
/// The driver is the caller's, so the same store runs on SQLite in a test, MySQL
/// in one service and PostgreSQL in another; this package never opens a socket.
/// The dialect is read from the driver once, at construction, because it decides
/// how a row is written (the upsert clause, the placeholder spelling) and nothing
/// later can change it.
///
/// Time is the app's: `logged_at` comes from the clock this store is given, so a
/// record written by a test is stamped by the test.
pub struct Store {
  driver : &@moondb.AsyncDriver
  dialect : @moondb.Dialect
  clock : () -> Int64
  tables : Tables
}

///|
/// A store on `driver`, stamping its records from `clock`.
pub fn Store::new(
  driver : &@moondb.AsyncDriver,
  clock~ : () -> Int64,
  tables~ : Tables = tables,
) -> Store {
  { driver, dialect: driver.dialect(), clock, tables, }
}

///|
/// The dialect the driver speaks.
pub fn Store::dialect(self : Store) -> @moondb.Dialect {
  self.dialect
}

///|
/// The tables this store writes.
pub fn Store::tables(self : Store) -> Tables {
  self.tables
}

///|
/// Create the two tables if they are not there yet.
///
/// Call it once, at start-up. It is not run implicitly: a store that ran DDL on
/// every write would put a schema change on the queue's hot path, and a
/// deployment that grants no DDL would fail every settle instead of failing
/// once, in the open.
pub async fn Store::ensure(self : Store) -> Unit raise @spec.Refused {
  let _ = self.execute(outcome_ddl(self.dialect, self.tables.outcome), [])
  let _ = self.execute(run_ddl(self.dialect, self.tables.run), [])
}

///|
async fn Store::execute(
  self : Store,
  sql : String,
  params : Array[@moondb.Value],
) -> @moondb.ExecResult raise @spec.Refused {
  self.driver.execute(sql, params) catch {
    error => raise @spec.Refused::Transport(got=error.to_string())
  }
}

///|
async fn Store::rows(
  self : Store,
  sql : String,
  params : Array[@moondb.Value],
) -> Array[@moondb.Row] raise @spec.Refused {
  self.driver.query(sql, params) catch {
    error => raise @spec.Refused::Transport(got=error.to_string())
  }
}

///|
/// A nullable text column, as a value to bind.
fn text_or_null(text : String?) -> @moondb.Value {
  match text {
    Some(text) => @moondb.Value::Text(text)
    None => @moondb.Value::Null
  }
}

///|
/// A text cell, refused in this library's terms when the driver cannot answer.
fn cell_text(row : @moondb.Row, index : Int32) -> String raise @spec.Refused {
  row.text(index) catch {
    error => raise @spec.Refused::Malformed(got=error.to_string())
  }
}

///|
fn cell_int(row : @moondb.Row, index : Int32) -> Int32 raise @spec.Refused {
  row.int(index) catch {
    error => raise @spec.Refused::Malformed(got=error.to_string())
  }
}

///|
fn cell_int64(row : @moondb.Row, index : Int32) -> Int64 raise @spec.Refused {
  row.int64(index) catch {
    error => raise @spec.Refused::Malformed(got=error.to_string())
  }
}

///|
fn cell_is_null(row : @moondb.Row, index : Int32) -> Bool raise @spec.Refused {
  row.is_null(index) catch {
    error => raise @spec.Refused::Malformed(got=error.to_string())
  }
}

///|
/// A JSON column.
fn cell_json(row : @moondb.Row, index : Int32) -> Json raise @spec.Refused {
  let text = cell_text(row, index)
  @json.parse(text) catch {
    error => raise @spec.Refused::Malformed(got=error.to_string())
  }
}

///|
/// One stored outcome, read back into the type the queue speaks.
fn decode_outcome(row : @moondb.Row) -> @spec.Outcome raise @spec.Refused {
  let state = @spec.State::of(cell_text(row, 0))
  let result = if cell_is_null(row, 1) { None } else { Some(cell_json(row, 1)) }
  let error = if cell_is_null(row, 2) { None } else { Some(cell_text(row, 2)) }
  { state, result, error, retries: cell_int(row, 3), at: cell_int64(row, 4), }
}

///|
pub extend Store with @spec.Backend::{fetch, store}

///|
/// Write what a run settled as, replacing the row of the same id.
///
/// The write is one statement, so a worker that is killed mid-write leaves the
/// previous state rather than half of the new one.
pub impl @spec.Backend for Store with fn store(
  self : Store,
  id : String,
  outcome : @spec.Outcome,
) -> Unit raise @spec.Refused {
  let sql = outcome_upsert(self.dialect, self.tables.outcome)
  let result = match outcome.result {
    Some(value) => @moondb.Value::Text(value.stringify())
    None => @moondb.Value::Null
  }
  let params = [
    @moondb.Value::Text(id),
    @moondb.Value::Text(outcome.state.code()),
    result,
    text_or_null(outcome.error),
    @moondb.Value::Int(outcome.retries),
    @moondb.Value::Int64(outcome.at),
  ]
  let _ = self.execute(sql, params)
}

///|
/// The outcome recorded for `id`, or `None` when the run has not settled.
pub impl @spec.Backend for Store with fn fetch(self : Store, id : String) -> @spec.Outcome? raise @spec.Refused {
  let sql = outcome_select(self.dialect, self.tables.outcome)
  let rows = self.rows(sql, [@moondb.Value::Text(id)])
  match rows {
    [] => None
    [row, ..] => Some(decode_outcome(row))
  }
}
