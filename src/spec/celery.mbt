// The message in the envelope Celery 5 puts on a queue.
//
// Celery speaks through kombu, and kombu does not put the message itself on the
// wire: it wraps it. The value in redis is one JSON object holding the task
// name and the run's id in `headers`, the routing in `properties`, and the
// arguments in `body` — which is itself base64 of `[args, kwargs, embed]`.
//
// This file converts both ways, so a task published by Celery can be run by a
// worker here and a task published here can be run by a Celery worker. What is
// converted is exactly the part both sides agree on: the task name, its
// argument, the id, the retry count and the two instants. Everything else in
// the envelope is filled in as Celery fills it in, or left empty.
//
// One limit worth naming: this library carries a single argument value, Celery
// carries a tuple of them. A message with one positional argument and no
// keyword arguments converts exactly in both directions; anything else is
// carried as the tuple, which round-trips through here but which a task
// expecting one argument will refuse.
// https://docs.celeryq.dev/en/stable/internals/protocol.html

///|
/// The message as Celery's envelope, ready to be pushed on a queue.
pub fn Message::to_celery(self : Message) -> Json {
  let positional = match self.args {
    Array(items) => Json::array(items)
    other => Json::array([other])
  }
  let embed = Json::object({
    "callbacks": Json::null(),
    "errbacks": Json::null(),
    "chain": match self.chain {
      [] => Json::null()
      items => Json::array(items.map(fn(next) { next.to_celery_body() }))
    },
    "chord": Json::null(),
  })
  let body = Json::array([positional, Json::object({}), embed]).stringify()
  let headers = Json::object({
    "lang": Json::string("moonbit"),
    "task": Json::string(self.name),
    "id": Json::string(self.id),
    "shadow": Json::null(),
    "eta": match self.eta {
      Some(at) => Json::string(instant_text(at))
      None => Json::null()
    },
    "expires": match self.expires {
      Some(at) => Json::string(instant_text(at))
      None => Json::null()
    },
    "group": Json::null(),
    "group_index": Json::null(),
    "retries": Json::number(self.retries.to_double()),
    "timelimit": Json::array([Json::null(), Json::null()]),
    "root_id": Json::string(self.id),
    "parent_id": Json::null(),
    "argsrepr": Json::string(self.args.stringify()),
    "kwargsrepr": Json::string("{}"),
    "origin": Json::string("moontask"),
    "ignore_result": Json::boolean(false),
  })
  let properties = Json::object({
    "correlation_id": Json::string(self.id),
    "reply_to": Json::string(self.id),
    "delivery_mode": Json::number(2),
    "delivery_info": Json::object({
      "exchange": Json::string(""),
      "routing_key": Json::string(self.queue),
    }),
    "priority": Json::number(self.priority.to_double()),
    "body_encoding": Json::string("base64"),
    "delivery_tag": Json::string(self.id),
  })
  Json::object({
    "body": Json::string(@base64.encode(@utf8.encode(body)[:])),
    "content-encoding": Json::string("utf-8"),
    "content-type": Json::string("application/json"),
    "headers": headers,
    "properties": properties,
  })
}

///|
/// The body of a message inside a chain, which Celery carries as a dict rather
/// than as a whole envelope.
fn Message::to_celery_body(self : Message) -> Json {
  Json::object({
    "task": Json::string(self.name),
    "args": match self.args {
      Array(items) => Json::array(items)
      other => Json::array([other])
    },
    "kwargs": Json::object({}),
    "options": Json::object({
      "task_id": Json::string(self.id),
      "queue": Json::string(self.queue),
    }),
    "subtask_type": Json::null(),
    "immutable": Json::boolean(false),
  })
}

///|
/// Read a Celery envelope back into a message.
///
/// `queue` is what the broker knows and the envelope names, so a message read
/// off a list is told which list it came from.
pub fn Message::from_celery(
  json : Json,
  queue~ : String,
) -> Message raise Refused {
  let fields = match json {
    Object(fields) => fields
    _ => raise Refused::Malformed(got="an envelope is an object")
  }
  let headers = match fields.get("headers") {
    Some(Object(headers)) => headers
    _ => raise Refused::Malformed(got="an envelope has headers")
  }
  let name = match headers.get("task") {
    Some(String(name)) => name
    _ => raise Refused::Malformed(got="the headers name no task")
  }
  let id = match headers.get("id") {
    Some(String(id)) => id
    _ => raise Refused::Malformed(got="the headers carry no id")
  }
  let retries = match headers.get("retries") {
    Some(Number(value, ..)) => value.to_int()
    _ => 0
  }
  let eta = match headers.get("eta") {
    Some(String(text)) => Some(instant_of(text))
    _ => None
  }
  let expires = match headers.get("expires") {
    Some(String(text)) => Some(instant_of(text))
    _ => None
  }
  let body = match fields.get("body") {
    Some(String(encoded)) =>
      @json.parse(@utf8.decode_lossy(@base64.decode(encoded)[:])) catch {
        _ => raise Refused::Malformed(got="the body is not base64 of json")
      }
    Some(other) => other
    None => raise Refused::Malformed(got="an envelope has a body")
  }
  let parts = match body {
    Array(parts) => parts
    _ => raise Refused::Malformed(got="the body is [args, kwargs, embed]")
  }
  let positional = match parts.length() {
    0 => []
    _ =>
      match parts[0] {
        Array(items) => items
        other => [other]
      }
  }
  let keyword = match parts.length() {
    1 => 0
    _ =>
      match parts[1] {
        Object(fields) => fields.length()
        _ => 0
      }
  }
  // One positional argument and no keyword arguments is the shape this library
  // uses; anything else is carried as the tuple.
  let args = if positional.length() == 1 && keyword == 0 {
    positional[0]
  } else {
    Json::array(positional)
  }
  {
    id,
    name,
    args,
    queue,
    priority: 0,
    eta,
    expires,
    retries,
    chain: [],
    chord: None,
  }
}

///|
/// An instant from the protocol's text, or zero when it does not parse: a
/// worker that finished the work should not be failed by a stamp it cannot read.
fn instant_of(text : String) -> Int64 {
  let moment = @moondate.parse(text) catch { _ => return 0L }
  match moment.timestamp() {
    Some(seconds) => (seconds * 1000.0).round().to_int64()
    None => 0L
  }
}

///|
/// The key Celery's result backend stores a run under.
pub fn celery_result_key(id : String) -> String {
  "celery-task-meta-\{id}"
}

///|
/// An outcome as Celery's result backend writes it, so a Celery client waiting
/// on `AsyncResult` reads what a worker here finished.
pub fn Outcome::to_celery(self : Outcome, id : String) -> Json {
  let done = Json::string(instant_text(self.at))
  let meta = {
    "task_id": Json::string(id),
    "status": Json::string(self.state.code()),
    "result": match self.result {
      Some(value) => value
      None => Json::null()
    },
    "traceback": match self.error {
      Some(error) => Json::string(error)
      None => Json::null()
    },
    "children": Json::array([]),
    "date_done": done,
  }
  Json::object(meta)
}

///|
/// An outcome read back from Celery's result backend.
pub fn Outcome::from_celery(json : Json) -> Outcome raise Refused {
  let fields = match json {
    Object(fields) => fields
    _ => raise Refused::Malformed(got="a result is an object")
  }
  let state = match fields.get("status") {
    Some(String(status)) => {
      let known = State::of(status) catch {
        _ => raise Refused::Malformed(got="unknown status: " + status)
      }
      known
    }
    _ => raise Refused::Malformed(got="a result has a status")
  }
  let result = match fields.get("result") {
    Some(Json::Null) | None => None
    Some(value) => Some(value)
  }
  let error = match fields.get("traceback") {
    Some(String(text)) => Some(text)
    _ => None
  }
  let at = match fields.get("date_done") {
    Some(String(text)) => instant_of(text)
    _ => 0L
  }
  { state, result, error, retries: 0, at, }
}
