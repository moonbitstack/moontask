// What the tests here need before they can look at anything: a clock they move,
// a connection to the server, and a prefix nobody else is using.

///|
/// A clock a test sets.
struct Manual {
  mut at : Int64
}

///|
fn Manual::new(at : Int64) -> Manual {
  { at, }
}

///|
fn Manual::now(self : Manual) -> Int64 {
  self.at
}

///|
/// An instant for keys unique to one test, so tests sharing a server do not
/// read each other's queues.
fn unique(prefix : String) -> String {
  "\{prefix}-\{@env.now()}"
}

///|
/// A connection to the server on `port`, or `None` when nothing listens and
/// nothing is required to.
async fn live(port : Int32) -> @socket.Connection? {
  let options = @moonredis.ClientOptions::at("127.0.0.1", port)
  // A port that is forwarded but not served accepts the connection and then
  // never answers, so the check is a round trip with a deadline rather than a
  // connect.
  let probe = @async.with_timeout_opt(2000, async fn() {
    let conn = @socket.Connection::connect(options)
    let client = @moonredis.Client::new(conn)
    let pong = client.ping()
    (conn, pong)
  })
  match probe {
    Some((conn, pong)) =>
      if pong == "PONG" {
        Some(conn)
      } else if @env.get_env_var("MOONTASK_REDIS_LIVE") is Some(_) {
        fail("redis on 127.0.0.1:\{port} answered \{pong}")
      } else {
        None
      }
    None => {
      if @env.get_env_var("MOONTASK_REDIS_LIVE") is Some(_) {
        fail("no redis on 127.0.0.1:\{port}")
      }
      return None
    }
  }
}

///|
/// A second connection to the server, for a test that must read on a socket of
/// its own.
async fn connect() -> @socket.Connection raise @moonredis.RedisError {
  @socket.Connection::connect(@moonredis.ClientOptions::at("127.0.0.1", 6379))
}

///|
/// Delete every key the tests may have left under `prefix`.
async fn clear(conn : @socket.Connection, prefix : String) -> Unit {
  let client = @moonredis.Client::new(conn)
  let mut cursor = "0"
  while true {
    let (next, keys) = client.scan(cursor, pattern="\{prefix}:*", count=256) catch {
      _ => return
    }
    if keys.length() > 0 {
      ignore(client.del(keys) catch { _ => 0L })
    }
    cursor = next
    if cursor == "0" {
      return
    }
  }
}

///|
/// Give the workers `ticks` milliseconds of real time to consume and settle.
async fn settle(ticks : Int32) -> Unit {
  let mut left = ticks
  while left > 0 {
    @async.sleep(1)
    left = left - 1
  }
}
