// Two directions between this library and a Celery 5 queue, one at a time.
//
//   example consume   wait up to 20 s for one message a Celery client published,
//                     run it here, and write the result where `AsyncResult` looks
//   example publish X  publish t.double(X) in Celery's envelope and wait for the
//                     answer a Celery worker writes
//
// A one-shot process on purpose: the first version ran a worker that never
// exited, and the harness hung waiting for it. Consuming one message and
// returning is the whole difference between a test that finishes and one that
// does not.

///|
fn now() -> Int64 {
  @env.now().reinterpret_as_int64()
}

///|
async fn connect() -> @socket.Connection {
  @socket.Connection::connect(@moonredis.ClientOptions::at("127.0.0.1", 6379))
}

///|
async fn main {
  let argv = @env.args()
  let mode = if argv.length() > 1 { argv[1] } else { "consume" }
  let conn = connect()
  let broker = @redis.Broker::celery(conn, clock=now, consumer="moontask")
  let app = @moontask.App::new(
    broker,
    backend=broker.store(),
    counter=broker.store(),
    clock=now,
  )
  let double = app.task("t.double", fn(value : Int32, _) { value * 2 })
  ignore(
    app.task("t.add", fn(values : Array[Int32], _) {
      let mut sum = 0
      for value in values {
        sum = sum + value
      }
      sum
    }),
  )
  if mode == "publish" {
    let value = if argv.length() > 2 {
      @string.parse_int(argv[2]) catch {
        _ => 21
      }
    } else {
      21
    }
    let id = "moon-\{now()}"
    let handle = double.send(value, id~, queue="celery")
    println("published \{id} t.double(\{value})")
    let answer = handle.get(timeout=25000L) catch {
      error => {
        println("no answer: \{error}")
        return
      }
    }
    println("celery answered \{answer}")
    return
  }
  println("waiting for one message on the celery queue")
  let worker = app.worker(queues=["celery"], concurrency=1)
  @async.with_task_group(async fn(group) {
    group.spawn_bg(async fn() { worker.run() }, no_wait=true)
    @wait.nap(4500)
    worker.stop(grace=100L)
  })
  println("consumed whatever was there")
}
