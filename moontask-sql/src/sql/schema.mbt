// The tables this store keeps, and the statements that create them.

///|
/// The names of the two tables.
///
/// They are configurable because a database is shared: an app that runs two
/// queues out of one schema gives each its own pair rather than mixing their
/// results.
pub(all) struct Tables {
  /// One row per run: its state, result and error, keyed by the run's id.
  outcome : String
  /// One row per workflow run, keyed by flow and logical instant.
  run : String
}

///|
/// The names the tables go by unless the caller names them otherwise.
pub let tables : Tables = { outcome: "moontask_outcome", run: "moontask_run", }

///|
/// `count` placeholders starting at `from`, in the dialect's spelling.
fn placeholders(
  dialect : @moondb.Dialect,
  from : Int32,
  count : Int32,
) -> String {
  let out = StringBuilder()
  for index = 0; index < count; index = index + 1 {
    if index > 0 {
      out.write_string(", ")
    }
    out.write_string(dialect.placeholder(from + index))
  }
  out.to_string()
}

///|
/// A text column holding a whole answer or a whole run state.
///
/// MySQL's `TEXT` tops out at 64 KiB, which a large result can pass, so the
/// column is the long form there; the other two dialects have one unbounded
/// text type and use it everywhere.
fn long_text(dialect : @moondb.Dialect) -> String {
  match dialect {
    @moondb.Mysql => "LONGTEXT"
    _ => "TEXT"
  }
}

///|
/// A primary-key column. MySQL indexes at most 3072 bytes under `utf8mb4`, so a
/// `VARCHAR(255)` pair would not fit; the other dialects index text as it is.
fn key_text(dialect : @moondb.Dialect) -> String {
  match dialect {
    @moondb.Mysql => "VARCHAR(191)"
    _ => "TEXT"
  }
}

///|
/// The statement that creates the outcome table if it is not there yet.
fn outcome_ddl(dialect : @moondb.Dialect, table : String) -> String {
  let key = key_text(dialect)
  let text = long_text(dialect)
  let retries = match dialect {
    @moondb.Mysql => "INT"
    _ => "INTEGER"
  }
  "CREATE TABLE IF NOT EXISTS " +
  table +
  " (id " +
  key +
  " PRIMARY KEY, state " +
  key +
  " NOT NULL, result " +
  text +
  ", error " +
  text +
  ", retries " +
  retries +
  " NOT NULL, updated_at BIGINT NOT NULL)"
}

///|
/// The statement that creates the run table if it is not there yet.
fn run_ddl(dialect : @moondb.Dialect, table : String) -> String {
  let key = key_text(dialect)
  let text = long_text(dialect)
  "CREATE TABLE IF NOT EXISTS " +
  table +
  " (flow " +
  key +
  " NOT NULL, thread " +
  key +
  " NOT NULL, instant BIGINT NOT NULL, status " +
  key +
  " NOT NULL, error " +
  text +
  ", state " +
  text +
  ", updated_at BIGINT NOT NULL, PRIMARY KEY (flow, thread))"
}

///|
/// The statement that writes one outcome, replacing the row of the same id.
///
/// The two spellings are not interchangeable: PostgreSQL and SQLite name the
/// incoming row `excluded` and need the conflict target, MySQL has a clause of
/// its own. Both keep the values as parameters rather than interpolating them.
fn outcome_upsert(dialect : @moondb.Dialect, table : String) -> String {
  let columns = "(id, state, result, error, retries, updated_at)"
  let values = "VALUES (" + placeholders(dialect, 1, 6) + ")"
  let head = "INSERT INTO " + table + " " + columns + " " + values
  match dialect {
    @moondb.Mysql =>
      head +
      " ON DUPLICATE KEY UPDATE state = VALUES(state), result = VALUES(result), error = VALUES(error), retries = VALUES(retries), updated_at = VALUES(updated_at)"
    _ =>
      head +
      " ON CONFLICT (id) DO UPDATE SET state = excluded.state, result = excluded.result, error = excluded.error, retries = excluded.retries, updated_at = excluded.updated_at"
  }
}

///|
/// The statement that reads one outcome back.
fn outcome_select(dialect : @moondb.Dialect, table : String) -> String {
  "SELECT state, result, error, retries, updated_at FROM " +
  table +
  " WHERE id = " +
  dialect.placeholder(1)
}

///|
/// The statement that writes one run, replacing the row of the same flow and
/// instant.
fn run_upsert(dialect : @moondb.Dialect, table : String) -> String {
  let columns = "(flow, thread, instant, status, error, state, updated_at)"
  let values = "VALUES (" + placeholders(dialect, 1, 7) + ")"
  let head = "INSERT INTO " + table + " " + columns + " " + values
  match dialect {
    @moondb.Mysql =>
      head +
      " ON DUPLICATE KEY UPDATE instant = VALUES(instant), status = VALUES(status), error = VALUES(error), state = VALUES(state), updated_at = VALUES(updated_at)"
    _ =>
      head +
      " ON CONFLICT (flow, thread) DO UPDATE SET instant = excluded.instant, status = excluded.status, error = excluded.error, state = excluded.state, updated_at = excluded.updated_at"
  }
}

///|
/// The statement that reads the newest runs of one flow. The instant is what
/// orders them: two runs of the same flow that started out of order still read
/// back in the order they were due.
fn run_select(dialect : @moondb.Dialect, table : String) -> String {
  "SELECT thread, instant, status, error, state FROM " +
  table +
  " WHERE flow = " +
  dialect.placeholder(1) +
  " ORDER BY instant DESC, thread DESC LIMIT " +
  dialect.placeholder(2)
}
