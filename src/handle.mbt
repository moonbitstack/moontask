// The handle: what a caller gets back from a send, and how it waits.

///|
/// How often a waiting handle asks the store again, in milliseconds.
let poll_interval : Int64 = 5

///|
/// A run, or a set of runs, a caller can wait on and look at.
///
/// The result type is the one the task returns: `get` decodes the stored JSON
/// back into it. `state` and `revoke` work on anything, since a caller that
/// wants to cancel a run does not care what it would have returned.
pub struct Handle[R] {
  app : App
  ids : Array[String]
  all : Bool
  codec : (Json) -> R raise @spec.Refused
}

///|
/// A handle over `ids` whose value decodes into `R`; `all` makes `get` answer
/// the list of results in the order of `ids`.
pub fn[R : FromJson] Handle::of(
  app : App,
  ids : Array[String],
  all : Bool,
) -> Handle[R] {
  {
    app,
    ids,
    all,
    codec: fn(value) raise @spec.Refused {
      @json.from_json(value) catch {
        error => raise @spec.Refused::Malformed(got=error.to_string())
      }
    },
  }
}

///|
/// A handle over `ids` of runs that already answer in JSON.
pub fn Handle::of_json(
  app : App,
  ids : Array[String],
  all : Bool,
) -> Handle[Json] {
  { app, ids, all, codec: fn(value) { value }, }
}

///|
/// The ids the handle waits on.
pub fn[R] Handle::ids(self : Handle[R]) -> Array[String] {
  self.ids
}

///|
/// Wait for the run and answer its value, or refuse with the reason it will not
/// arrive: `Refused::Failed` when a body raised on its last attempt,
/// `Refused::Timeout` when `timeout` milliseconds passed first, and
/// `Refused::Missing` when the app keeps no results.
pub async fn[R] Handle::get(
  self : Handle[R],
  timeout? : Int64,
) -> R raise @spec.Refused {
  let backend = match self.app.backend {
    Some(backend) => backend
    None => raise @spec.Refused::Missing(kind="backend")
  }
  let limit = timeout.unwrap_or(0L)
  let deadline = if timeout is Some(_) {
    Some(self.app.now() + limit)
  } else {
    None
  }
  while true {
    let mut settled = true
    let values = []
    let mut failure = None
    let mut last_retries = 0
    for id in self.ids {
      match backend.fetch(id) {
        Some(record) =>
          match record.state {
            @spec.Succeeded =>
              values.push(record.result.unwrap_or(Json::null()))
            @spec.Failed => {
              settled = false
              if failure is None {
                failure = Some(record.error.unwrap_or("failed"))
                last_retries = record.retries
              }
            }
            @spec.Revoked => {
              settled = false
              if failure is None {
                failure = Some("revoked")
              }
            }
            _ => settled = false
          }
        None => settled = false
      }
    }
    if settled {
      if self.all {
        return (self.codec)(Json::array(values))
      }
      match values {
        [value, ..] => return (self.codec)(value)
        [] => raise @spec.Refused::Empty(kind="result")
      }
    }
    match failure {
      Some(message) =>
        raise @spec.Refused::Failed(got=message, retries=last_retries)
      None => ()
    }
    match deadline {
      Some(at) =>
        if self.app.now() >= at {
          raise @spec.Refused::Timeout(limit~)
        }
      None => ()
    }
    @wait.nap(poll_interval.to_int())
  } nobreak {
    raise @spec.Refused::Timeout(limit~)
  }
}

///|
/// What the run is doing now, without waiting for it.
///
/// A set reports `Failed` once any part failed, `Revoked` once any part was
/// revoked, `Succeeded` once every part settled, and the state of the first
/// unsettled run otherwise.
pub async fn[R] Handle::state(
  self : Handle[R],
) -> @spec.State raise @spec.Refused {
  let backend = match self.app.backend {
    Some(backend) => backend
    None => raise @spec.Refused::Missing(kind="backend")
  }
  let mut settled = 0
  let mut first_unsettled : @spec.State? = None
  for id in self.ids {
    match backend.fetch(id) {
      Some(record) =>
        match record.state {
          @spec.Failed => return @spec.Failed
          @spec.Revoked => return @spec.Revoked
          @spec.Succeeded => settled = settled + 1
          state => if first_unsettled is None { first_unsettled = Some(state) }
        }
      None =>
        if first_unsettled is None {
          first_unsettled = Some(@spec.Pending)
        }
    }
  }
  if settled == self.ids.length() {
    return @spec.Succeeded
  }
  first_unsettled.unwrap_or(@spec.Pending)
}

///|
/// Cancel the run or the set: it will not be sent again, and with `terminate`
/// a worker in this process that is running it right now stops.
///
/// Terminating reaches only this process's workers; a run already going in
/// another process is left to finish, since nothing else is reachable.
pub async fn[R] Handle::revoke(
  self : Handle[R],
  terminate~ : Bool = false,
) -> Unit raise @spec.Refused {
  let backend = match self.app.backend {
    Some(backend) => backend
    None => raise @spec.Refused::Missing(kind="backend")
  }
  for id in self.ids {
    let revoked : @spec.Outcome = {
      state: @spec.Revoked,
      result: None,
      error: None,
      retries: 0,
      at: self.app.now(),
    }
    backend.store(id, revoked)
    if terminate {
      match self.app.running.get(id) {
        Some(task) => task.cancel()
        None => ()
      }
    }
  }
}
