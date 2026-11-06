// One connection, many tasks.
//
// A redis connection carries one reply stream, so two tasks reading it at once
// is not a race the server can survive: the reader that loses sees the other's
// reply, and the event loop aborts the process rather than return a wrong value.
// A worker with four consumers and a caller waiting on a handle is exactly that
// situation, so every command goes through one lock.
//
// The lock is held for the length of one command. That is why the broker never
// blocks inside one: a consumer that has nothing to take asks and comes back a
// few milliseconds later, rather than waiting on the server with the connection
// held.

///|
/// A connection that lets one task at a time through.
pub struct SerialConn {
  inner : &@moonredis.RedisConn
  lock : @async.Mutex
}

///|
/// `conn`, one command at a time.
pub fn SerialConn::new(conn : &@moonredis.RedisConn) -> SerialConn {
  { inner: conn, lock: @async.Mutex::Mutex() }
}

///|
pub impl @moonredis.RedisConn for SerialConn with fn execute(
  self : SerialConn,
  args : Array[Bytes],
) -> @resp.RespValue raise @moonredis.RedisError {
  self.lock.acquire()
  let outcome : Result[@resp.RespValue, @moonredis.RedisError] = try {
    Ok(self.inner.execute(args))
  } catch {
    error => Err(error)
  }
  self.lock.release()
  match outcome {
    Ok(reply) => reply
    Err(error) => raise error
  }
}

///|
pub impl @moonredis.RedisConn for SerialConn with fn execute_all(
  self : SerialConn,
  commands : Array[Array[Bytes]],
) -> Array[@resp.RespValue] raise @moonredis.RedisError {
  self.lock.acquire()
  let outcome : Result[Array[@resp.RespValue], @moonredis.RedisError] = try {
    Ok(self.inner.execute_all(commands))
  } catch {
    error => Err(error)
  }
  self.lock.release()
  match outcome {
    Ok(replies) => replies
    Err(error) => raise error
  }
}

///|
pub extend SerialConn with @moonredis.RedisConn::{execute, execute_all}
