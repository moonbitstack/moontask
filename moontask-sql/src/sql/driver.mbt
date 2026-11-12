// The adapter that lets a driver answering in the same call sit where the store
// expects one that answers from a task.

///|
/// A synchronous driver, read as an asynchronous one.
///
/// `moondb` has two contracts: one where a call answers before it returns, and
/// one where it answers from a task. A local database is the first — a statement
/// against a file has nothing to wait for — so its call runs inline rather than
/// being handed to a thread. The store then writes against one shape whichever
/// it was given.
///
/// A server across a socket is not this: it has a driver of its own that answers
/// from a task, and handing its blocking contract here would hold the task for
/// as long as the reply takes.
pub struct Inline {
  inner : &@moondb.Driver
}

///|
pub fn Inline::new(inner : &@moondb.Driver) -> Inline {
  { inner, }
}

///|
pub extend Inline with @moondb.AsyncDriver::{
  dialect,
  capabilities,
  execute,
  query,
  query_stream,
  prepare,
  begin,
  commit,
  rollback,
  close,
  ping,
}

///|
pub impl @moondb.AsyncDriver for Inline with fn dialect(self) {
  self.inner.dialect()
}

///|
pub impl @moondb.AsyncDriver for Inline with fn capabilities(self) {
  self.inner.capabilities()
}

///|
pub impl @moondb.AsyncDriver for Inline with fn execute(self, sql, params) {
  self.inner.execute(sql, params)
}

///|
pub impl @moondb.AsyncDriver for Inline with fn query(self, sql, params) {
  self.inner.query(sql, params)
}

///|
pub impl @moondb.AsyncDriver for Inline with fn begin(self) {
  self.inner.begin()
}

///|
pub impl @moondb.AsyncDriver for Inline with fn commit(self) {
  self.inner.commit()
}

///|
pub impl @moondb.AsyncDriver for Inline with fn rollback(self) {
  self.inner.rollback()
}

///|
pub impl @moondb.AsyncDriver for Inline with fn close(self) {
  self.inner.close()
}

///|
pub impl @moondb.AsyncDriver for Inline with fn ping(self) {
  self.inner.ping()
}
