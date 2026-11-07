// The run records: one row per workflow run, written as the run ends.

///|
/// One stored run, read back into the type the flow speaks.
///
/// The flow is not in the row: it is the column every query here filters on, so
/// the caller already has it.
fn decode_run(
  flow : String,
  row : @moondb.Row,
) -> @flow.Run raise @spec.Refused {
  let thread = cell_text(row, 0)
  let at = cell_int64(row, 1)
  let status = @flow.Status::of(
    cell_text(row, 2),
    got=if cell_is_null(row, 3) { "" } else { cell_text(row, 3) },
  )
  let state = if cell_is_null(row, 4) {
    Json::null()
  } else {
    cell_json(row, 4)
  }
  { flow, thread, at, state, status, }
}

///|
pub extend Store with @flow.Recorder::{record, runs}

///|
/// Write one run, replacing the row of the same flow and instant.
///
/// A run that is rerun for the same instant — a backfill asked for a window
/// twice, a resumed run that ends differently — leaves what the last one did,
/// which is what "what happened last" has to mean.
pub impl @flow.Recorder for Store with fn record(self : Store, run : @flow.Run) -> Unit raise {
  let sql = run_upsert(self.dialect, self.tables.run)
  let (status, error) = match run.status {
    @flow.Status::Succeeded => ("SUCCESS", None)
    @flow.Status::Failed(got~) => ("FAILURE", Some(got))
  }
  let params = [
    @moondb.Value::Text(run.flow),
    @moondb.Value::Text(run.thread),
    @moondb.Value::Int64(run.at),
    @moondb.Value::Text(status),
    text_or_null(error),
    @moondb.Value::Text(run.state.stringify()),
    @moondb.Value::Int64((self.clock)()),
  ]
  let _ = self.execute(sql, params)
}

///|
/// The newest runs of `flow`, newest first.
pub impl @flow.Recorder for Store with fn runs(
  self : Store,
  flow : String,
  limit~ : Int32,
) -> Array[@flow.Run] raise {
  let sql = run_select(self.dialect, self.tables.run)
  let rows = self.rows(sql, [
    @moondb.Value::Text(flow),
    @moondb.Value::Int(limit),
  ])
  let runs = []
  for row in rows {
    runs.push(decode_run(flow, row))
  }
  runs
}
