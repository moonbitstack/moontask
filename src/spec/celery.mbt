// The message in the envelope Celery 5 puts on a queue.
//
// Celery speaks through kombu, and kombu does not put the message itself on the
// wire: it wraps it. The value in redis is one JSON object holding the task
// name and the run's id in `headers`, the routing in `properties`, and the
// arguments in `body` — which is itself base64 of `[args, kwargs, embed]`.
//
// This file converts both ways, so a task published by Celery can be run by a
// worker here and a task published here can be run by a Celery worker. The parts
// that carry meaning are converted; the rest is filled in as Celery fills it in.
//
// Three things are worth naming, because they are where the two models differ:
//
// - **Arguments.** This library carries one argument value, Celery a tuple of
//   them. A message whose value is an array is read as the tuple; a message with
//   `single_arg` set is one argument that happens to be an array, which is what
//   a chord's callback receives.
// - **A chain** is not a separate message: each message carries the rest of its
//   chain in the body's `embed.chain`, and the worker that finishes a link
//   publishes the next. A link that is not `immutable` is called with the link
//   before it's result prepended to its own arguments, which is Celery's rule.
// - **A chord** is a counter both sides have to agree on: a part names its group
//   and its index in the headers, and carries the callback as a signature in the
//   body's `embed.chord`. The join itself lives in the result backend — see
//   `Chord` and the redis `Counter`, which writes Celery's own join keys.
// https://docs.celeryq.dev/en/stable/internals/protocol.html

///|
/// The message as Celery's envelope, ready to be pushed on a queue.
pub fn Message::to_celery(self : Message) -> Json {
  let body = @moonjson.dumps(
    Json::array([
      self.positional(),
      Json::object({}),
      Json::object({
        "callbacks": Json::null(),
        "errbacks": Json::null(),
        "chain": match self.chain {
          [] => Json::null()
          items => Json::array(items.map(fn(next) { next.to_celery_body() }))
        },
        "chord": match self.chord {
          Some(chord) => chord.callback.to_celery_body()
          None => Json::null()
        },
      }),
    ]),
  )
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
    "group": match self.chord {
      Some(chord) => Json::string(chord.key)
      None => Json::null()
    },
    "group_index": match self.chord {
      Some(chord) => Json::number(chord.index.to_double())
      None => Json::null()
    },
    "retries": Json::number(self.retries.to_double()),
    "timelimit": Json::array([Json::null(), Json::null()]),
    "root_id": Json::string(self.id),
    "parent_id": Json::null(),
    "argsrepr": Json::string(@moonjson.dumps(self.args)),
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
/// The arguments as Celery's positional tuple.
fn Message::positional(self : Message) -> Json {
  if self.single_arg {
    Json::array([self.args])
  } else {
    match self.args {
      Array(items) => Json::array(items)
      other => Json::array([other])
    }
  }
}

///|
/// The body of a message inside a chain or a chord, which Celery carries as a
/// signature dict rather than as a whole envelope.
fn Message::to_celery_body(self : Message) -> Json {
  Json::object({
    "task": Json::string(self.name),
    "args": self.positional(),
    "kwargs": Json::object({}),
    "options": Json::object({
      "task_id": Json::string(self.id),
      "queue": Json::string(self.queue),
    }),
    "subtask_type": Json::null(),
    // A step of a chain built here is called with what it declares; Celery's
    // default would hand it the previous link's result as well.
    "immutable": Json::boolean(self.immutable),
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
      @moonjson.loads(
        @utf8.decode_lossy(@base64.decode(encoded)[:]),
        @moonjson.strict,
      ) catch {
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
  let embed = match parts.length() {
    3 =>
      match parts[2] {
        Object(fields) => Some(fields)
        _ => None
      }
    _ => None
  }
  let chain = match embed.bind(fn(fields) { fields.get("chain") }) {
    Some(Array(items)) =>
      items.map(fn(item) raise Refused { Message::of_celery_body(item, queue~) })
    _ => []
  }
  let chord = match embed.bind(fn(fields) { fields.get("chord") }) {
    Some(Object(_)) => {
      let key = match headers.get("group") {
        Some(String(key)) => key
        _ => raise Refused::Malformed(got="a chord part names no group")
      }
      let index = match headers.get("group_index") {
        Some(Number(value, ..)) => value.to_int()
        _ => 0
      }
      let callback = Message::of_celery_body(
        embed.bind(fn(fields) { fields.get("chord") }).unwrap(),
        queue~,
      )
      // `total` is not in the envelope: Celery keeps the size in its result
      // backend, and the counter that joins the parts reads it from there.
      Some({ key, index, total: 0, ids: [], callback, })
    }
    _ => None
  }
  // One positional argument is that value, and it travels back as one argument;
  // more than one is the tuple, which travels back as the tuple.
  let single_arg = positional.length() == 1
  let args = if single_arg {
    positional[0]
  } else if keyword == 0 {
    Json::array(positional)
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
    chain,
    chord,
    single_arg,
    immutable: false,
  }
}

///|
/// A chain link or a chord's callback, read from the signature dict Celery puts
/// inside a body. Its queue comes from the signature's own options when it names
/// one, and from the envelope around it otherwise.
fn Message::of_celery_body(
  json : Json,
  queue~ : String,
) -> Message raise Refused {
  let fields = match json {
    Object(fields) => fields
    _ => raise Refused::Malformed(got="a signature is an object")
  }
  let name = match fields.get("task") {
    Some(String(name)) => name
    _ => raise Refused::Malformed(got="a signature names no task")
  }
  let options = match fields.get("options") {
    Some(Object(options)) => options
    _ => Map([])
  }
  let id = match options.get("task_id") {
    Some(String(id)) => id
    _ => ""
  }
  let queue = match options.get("queue") {
    Some(String(queue)) => queue
    _ => queue
  }
  let positional = match fields.get("args") {
    Some(Array(items)) => items
    Some(other) => [other]
    None => []
  }
  let single_arg = positional.length() == 1
  {
    id,
    name,
    args: if single_arg {
      positional[0]
    } else {
      Json::array(positional)
    },
    queue,
    priority: 0,
    eta: None,
    expires: None,
    retries: 0,
    chain: [],
    chord: None,
    single_arg,
    immutable: match fields.get("immutable") {
      Some(True) => true
      Some(False) => false
      _ => false
    },
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
/// The keys a chord's join lives under in Celery's redis backend: the results
/// as they arrive (`.j`, a sorted set scored by the part's index), the
/// adjustment to the size (`.t`), and the size itself (`.s`).
pub fn celery_group_key(group : String, part : String) -> String {
  "celery-taskset-meta-\{group}\{part}"
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
