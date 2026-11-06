// What the management interface answers, driven through ASGI.

///|
/// The value under `key` of a report object.
fn Json::field(self : Json, key : String) -> Json? {
  if self is Object(fields) {
    fields.get(key)
  } else {
    None
  }
}

///|
/// A clock a test sets.
priv struct Manual {
  at : Int64
}

///|
fn set_up() -> (@moontask.App, @moontask.MemoryBroker) {
  let clock : Manual = { at: 1_000_000L, }
  let broker = @moontask.MemoryBroker::new(clock=fn() { clock.at })
  let app = @moontask.App::new(broker, backend=broker, counter=broker, clock=fn() {
    clock.at
  })
  (app, broker)
}

///|
async test "the page is served and names the live stream" {
  let (app, broker) = set_up()
  let admin = @admin.Admin::new(app, inspector=broker)
  let client = @testing.Client::new(admin.to_asgi())
  let page = client.get("/")
  assert_eq(page.status, 200)
  assert_true(page.text().contains("moontask"))
  assert_true(page.text().contains("/api/events"))
  match page.header("content-type") {
    Some(kind) => assert_true(kind.contains("text/html"))
    None => fail("the page has no content type")
  }
}

///|
async test "the task report lists what is registered and how it runs" {
  let (app, broker) = set_up()
  let _ = app.task("mail.send", fn(_ : Int32, _) {  }, policy={
    ..@moontask.policy,
    queue: "mail",
    retries: 3,
    acks_late: true,
  })
  let admin = @admin.Admin::new(app, inspector=broker)
  let client = @testing.Client::new(admin.to_asgi())
  let response = client.get("/api/tasks")
  assert_eq(response.status, 200)
  let tasks = response.json() catch { _ => fail("the report is json") }
  match tasks {
    Array(items) => {
      assert_eq(items.length(), 1)
      assert_eq(items[0].field("name"), Some(Json::string("mail.send")))
      assert_eq(items[0].field("queue"), Some(Json::string("mail")))
      assert_eq(items[0].field("retries"), Some(Json::number(3.0)))
      assert_eq(items[0].field("acks_late"), Some(Json::boolean(true)))
    }
    _ => fail("the report is a list")
  }
}

///|
async test "the queue report shows what waits" {
  let (app, broker) = set_up()
  let _ = app.task("later", fn(_ : Unit, _) -> Unit {  })
  let _ = app.send("later", Json::null())
  let admin = @admin.Admin::new(app, inspector=broker)
  let client = @testing.Client::new(admin.to_asgi())
  let queues = client.get("/api/queues").json() catch {
      _ => fail("the report is json")
    }
  match queues {
    Array(items) => {
      assert_eq(items.length(), 1)
      assert_eq(items[0].field("name"), Some(Json::string("default")))
      assert_eq(items[0].field("ready"), Some(Json::number(1.0)))
    }
    _ => fail("the report is a list")
  }
}

///|
async test "sending a task from the page queues it" {
  let (app, broker) = set_up()
  let seen = []
  let _ = app.task("echo", fn(value : Int32, _) -> Unit { seen.push(value) })
  let admin = @admin.Admin::new(app, inspector=broker)
  let client = @testing.Client::new(admin.to_asgi())
  let sent = client.post("/api/tasks/echo", body=b"7")
  assert_eq(sent.status, 202)
  let answer = sent.json() catch { _ => fail("the answer is json") }
  let id = match answer.field("id") {
    Some(Json::String(id)) => id
    _ => fail("the answer names the run")
  }
  assert_eq(broker.queues()[0].ready, 1L)
  assert_eq(seen.length(), 0)
  // The run the page started is the one the queue shows.
  match broker.peek("default", limit=5) {
    [message, ..] => assert_eq(message.id, id)
    [] => fail("nothing was queued")
  }
}

///|
async test "sending to a name nobody registered is a miss, not a queue entry" {
  let (app, broker) = set_up()
  let admin = @admin.Admin::new(app, inspector=broker)
  let client = @testing.Client::new(admin.to_asgi())
  let sent = client.post("/api/tasks/nobody", body=b"null")
  assert_eq(sent.status, 404)
  assert_eq(broker.queues().length(), 0)
}

///|
async test "the result report shows the state a run is in" {
  let (app, broker) = set_up()
  let _ = app.task("slow", fn(_ : Unit, _) -> Unit {  })
  let handle = app.send("slow", Json::null())
  let id = handle.ids()[0]
  let admin = @admin.Admin::new(app, inspector=broker)
  let client = @testing.Client::new(admin.to_asgi())
  let pending = client.get("/api/results/" + id).json() catch {
      _ => fail("the report is json")
    }
  assert_eq(pending.field("state"), Some(Json::string("PENDING")))
  broker.store(id, @spec.outcome_succeeded(Json::number(1.0), 0, 1_000_000L))
  let done = client.get("/api/results/" + id).json() catch {
      _ => fail("the report is json")
    }
  assert_eq(done.field("state"), Some(Json::string("SUCCESS")))
}

///|
async test "revoking from the page marks the run" {
  let (app, broker) = set_up()
  let _ = app.task("slow", fn(_ : Unit, _) -> Unit {  })
  let handle = app.send("slow", Json::null())
  let id = handle.ids()[0]
  let admin = @admin.Admin::new(app, inspector=broker)
  let client = @testing.Client::new(admin.to_asgi())
  let response = client.post("/api/results/" + id + "/revoke")
  assert_eq(response.status, 200)
  match broker.fetch(id) {
    Some(outcome) => assert_eq(outcome.state, @spec.Revoked)
    None => fail("the run was not marked")
  }
}

///|
async test "the event stream sends the counts it promised" {
  let (app, broker) = set_up()
  let _ = app.task("later", fn(_ : Unit, _) -> Unit {  })
  let _ = app.send("later", Json::null())
  let admin = @admin.Admin::new(app, inspector=broker, frames=2, interval=1L)
  let client = @testing.Client::new(admin.to_asgi())
  let response = client.get("/api/events")
  assert_eq(response.status, 200)
  match response.header("content-type") {
    Some(kind) => assert_true(kind.contains("text/event-stream"))
    None => fail("the stream has no content type")
  }
  let text = response.text()
  assert_true(text.contains("data: "))
  assert_true(text.contains("\"ready\":1"))
}

///|
async test "an unknown route answers with a miss" {
  let (app, broker) = set_up()
  let admin = @admin.Admin::new(app, inspector=broker)
  let client = @testing.Client::new(admin.to_asgi())
  assert_eq(client.get("/api/nothing").status, 404)
}
