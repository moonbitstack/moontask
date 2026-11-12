// One process, one move: the bridge between this library and a Celery 5 queue.
//
// Every mode does one thing and exits, so a test can drive it with a deadline
// instead of waiting on a worker that never stops:
//
//   example consume <queue> [count] [seconds]  run what is queued, then stop
//   example publish <queue> <value>            t.double(value), and wait for it
//   example chain <q1> <q2>                    a two-link chain, published here
//   example chord <parts> <callback>           a chord of two parts, published here
//   example fanout <queue>                     a map and a starmap, published here
//   example claim <queue>                      take one message and die holding it
//   example recover <seconds>                  hand back expired reservations
//   example eta <queue> <seconds>              publish with an eta in the future
//
// The queue is Celery's list and the payload is Celery's envelope, so what a
// Celery worker does with what this prints is the whole test.

///|
fn now() -> Int64 {
  @env.now().reinterpret_as_int64()
}

///|
async fn connect() -> @socket.Connection {
  @socket.Connection::connect(@moonredis.ClientOptions::at("127.0.0.1", 6379))
}

///|
fn arg(argv : Array[String], at : Int32, fallback : String) -> String {
  if at < argv.length() {
    argv[at]
  } else {
    fallback
  }
}

///|
fn number(text : String, fallback : Int32) -> Int32 {
  @string.parse_int(text[:]) catch {
    _ => fallback
  }
}

///|
fn say(fields : Map[String, Json]) -> Unit {
  println(Json::object(fields).stringify())
}

///|
async fn main {
  let argv = @env.args()
  let mode = arg(argv, 1, "consume")
  let conn = connect()
  if mode == "recover" {
    let seconds = number(arg(argv, 2, "60"), 60)
    let broker = @redis.Broker::celery(
      conn,
      clock=now,
      consumer="recoverer",
      visibility=seconds.to_int64() * 1000L,
    )
    say({ "moved": Json::number(broker.recover().to_double()) })
    return
  }
  let broker = @redis.Broker::celery(conn, clock=now, consumer="moontask")
  let app = @moontask.App::new(
    broker,
    backend=broker.store(),
    counter=broker.store(),
    clock=now,
  )
  let ran = []
  // Registered for a chain a Celery client started: the head there is t.step1.
  ignore(
    app.task("t.step1", fn(value : Int32, _) {
      ran.push("t.step1(\{value})@\{now()}")
      value * 10
    }),
  )
  // Two arguments, so the list a fan-out publishes decodes into a list here.
  ignore(
    app.task("t.add", fn(values : Array[Int32], _) {
      let shown = Json::array(values.map(fn(v) { Json::number(v.to_double()) }))
      ran.push("t.add(" + shown.stringify() + ")@\{now()}")
      let mut total = 0
      for value in values {
        total = total + value
      }
      total
    }),
  )
  let double = app.task("t.double", fn(value : Int32, _) {
    ran.push("t.double(\{value})@\{now()}")
    value * 2
  })
  let queue = arg(argv, 2, "celery")
  let third = arg(argv, 3, "1")
  let fourth = arg(argv, 4, "10")
  match mode {
    "publish" => {
      let value = number(third, 21)
      let id = "moon-\{now()}"
      let handle = double.send(value, id~, queue~)
      say({ "published": Json::string(id) })
      let answer = handle.get(timeout=25000L) catch {
        error => {
          say({ "error": Json::string(@debug.to_string(error)) })
          return
        }
      }
      say({ "answered": answer })
    }
    "chain" => {
      // The first link carries the second with it. Neither is immutable, which
      // is Celery's rule: the second is called with the first one's result as
      // its first argument.
      let stamp = now()
      let second = {
        ..@spec.Message::new("chain-b-\{stamp}", "t.step2", Json::array([])),
        queue: third,
        immutable: false,
      }
      let first = {
        ..@spec.Message::new("chain-a-\{stamp}", "t.step1", Json::number(5)),
        queue,
        chain: [second],
      }
      broker.publish(queue, first)
      say({ "published": Json::string("chain-a-\{stamp}") })
    }
    "chord" => {
      // Two parts and a callback. The parts run here, so this side writes the
      // join; the callback goes to the queue a Celery worker is listening on.
      let part = app.task("t.step1", fn(value : Int32, _) { value * 10 }, policy={
        ..@moontask.policy,
        queue,
      })
      let sum = app.task(
        "t.sum",
        fn(values : Array[Int32], _) {
          let mut total = 0
          for value in values {
            total = total + value
          }
          ran.push("t.sum")
          total
        },
        policy={ ..@moontask.policy, queue: third, },
      )
      let handle = @moontask.chord([part.sig(1), part.sig(2)], sum.sig([])).send(
        app,
      )
      say({ "published": Json::string(handle.ids()[0]) })
    }
    "fanout" => {
      // One run per item, and one run per tuple. The first is a list of single
      // arguments, the second of two-argument lists; a Celery worker spreads the
      // second into the function's parameters and takes the first as one.
      let step = app.task("t.step1", fn(value : Int32, _) { value * 10 }, policy={
        ..@moontask.policy,
        queue,
      })
      let add = app.task(
        "t.add",
        fn(values : Array[Int32], _) {
          let mut total = 0
          for value in values {
            total = total + value
          }
          total
        },
        policy={ ..@moontask.policy, queue, },
      )
      let mapped = step.map([1, 2, 3])
      let starred = add.starmap([[1, 2], [3, 4]])
      say({
        "published": Json::number(
          (mapped.links().length() + starred.links().length()).to_double(),
        ),
      })
      let _ = mapped.send(app)
      let _ = starred.send(app)
    }
    "claim" => {
      // Take one and leave: the reservation stays in Celery's table, which is
      // what a worker that was killed leaves behind.
      let delivery = broker.consume([queue], prefetch=1)
      say({ "claimed": Json::string(delivery.receipt) })
    }
    "eta" => {
      let seconds = number(third, 3)
      let message = {
        ..@spec.Message::new("eta-\{now()}", "t.step1", Json::number(5)),
        queue,
        eta: Some(now() + seconds.to_int64() * 1000L),
      }
      broker.publish(queue, message)
      say({
        "published": Json::string(message.id),
        "due": Json::number(message.eta.unwrap_or(0L).to_double()),
      })
    }
    _ => {
      let count = number(third, 1)
      let seconds = number(fourth, 10)
      let worker = app.worker(queues=[queue], concurrency=1)
      let deadline = now() + seconds.to_int64() * 1000L
      @async.with_task_group(async fn(group) {
        group.spawn_bg(async fn() { worker.run() }, no_wait=true)
        while ran.length() < count && now() < deadline {
          @wait.nap(50)
        }
        worker.stop(grace=200L)
      })
      say({
        "ran": Json::number(ran.length().to_double()),
        "tasks": Json::array(ran.map(fn(name) { Json::string(name) })),
      })
    }
  }
}
