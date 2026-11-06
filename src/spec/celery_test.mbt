// The envelope, in both directions, against the shape Celery 5.5 puts on the
// wire (captured with `redis-cli lrange celery 0 0` after a real `apply_async`).

///|
test "a message becomes the envelope Celery reads" {
  let message = @spec.Message::new("probe-1", "t.add", Json::number(2))
  let envelope = message.to_celery()
  let fields = match envelope {
    Object(fields) => fields
    _ => fail("the envelope is an object")
  }
  // the task name and the id are in the headers, not the body
  let headers = match fields.get("headers") {
    Some(Object(headers)) => headers
    _ => fail("the envelope has headers")
  }
  assert_eq(headers.get("task"), Some(Json::string("t.add")))
  assert_eq(headers.get("id"), Some(Json::string("probe-1")))
  assert_eq(headers.get("retries"), Some(Json::number(0.0)))
  // the body is base64 of [args, kwargs, embed], with the argument in a tuple
  let body = match fields.get("body") {
    Some(String(text)) => @utf8.decode_lossy(@base64.decode(text)[:])
    _ => fail("the body is base64 text")
  }
  let parts = match @json.parse(body) {
    Array(parts) => parts
    _ => fail("the body is [args, kwargs, embed]")
  }
  assert_eq(parts.length(), 3)
  match parts[0] {
    Array(args) => {
      assert_eq(args.length(), 1)
      assert_eq(args[0], Json::number(2.0))
    }
    _ => fail("the positional arguments are a tuple")
  }
  match parts[1] {
    Object(kwargs) => assert_eq(kwargs.length(), 0)
    _ => fail("the keyword arguments are an object")
  }
}

///|
test "an envelope reads back as the message it carried" {
  let message = @spec.Message::new("run-7", "t.add", Json::number(3))
  let back = @spec.Message::from_celery(message.to_celery(), queue="celery")
  assert_eq(back.id, "run-7")
  assert_eq(back.name, "t.add")
  assert_eq(back.args, Json::number(3))
  assert_eq(back.queue, "celery")
  assert_eq(back.retries, 0)
}

///|
test "the retry count and the eta survive the envelope" {
  let message = {
    ..@spec.Message::new("run-8", "t.add", Json::number(1)),
    retries: 2,
    eta: Some(1_700_000_000_000L),
  }
  let back = @spec.Message::from_celery(message.to_celery(), queue="celery")
  assert_eq(back.retries, 2)
  assert_eq(back.eta, Some(1_700_000_000_000L))
}

///|
test "a failure reads back as failure with its message" {
  let outcome = @spec.outcome_failed("boom", 1, 1_700_000_000_000L)
  let back = @spec.Outcome::from_celery(outcome.to_celery("run-9"))
  assert_eq(back.state, @spec.Failed)
  assert_eq(back.error, Some("boom"))
}

///|
test "a success reads back with its value" {
  let outcome = @spec.outcome_succeeded(
    Json::string("done"),
    0,
    1_700_000_000_000L,
  )
  let back = @spec.Outcome::from_celery(outcome.to_celery("run-9"))
  assert_eq(back.state, @spec.Succeeded)
  assert_eq(back.result, Some(Json::string("done")))
}

///|
/// The body of an envelope, decoded: `[args, kwargs, embed]`.
fn body_of(envelope : Json) -> Array[Json] raise {
  let fields = match envelope {
    Object(fields) => fields
    _ => fail("the envelope is an object")
  }
  let text = match fields.get("body") {
    Some(String(text)) => @utf8.decode_lossy(@base64.decode(text)[:])
    _ => fail("the body is base64 text")
  }
  match @json.parse(text) {
    Array(parts) => parts
    _ => fail("the body is a triple")
  }
}

///|
/// The `embed` object of an envelope.
fn embed_of(envelope : Json) -> Map[String, Json] raise {
  let parts = body_of(envelope)
  match parts[2] {
    Object(fields) => fields
    _ => fail("the third part of the body is the embed")
  }
}

///|
test "a list of results travels as one argument, not as the argument list" {
  // A chord's callback is called with the list of its parts' results as one
  // argument, which is an array — and an array in `args` normally is the tuple.
  let callback = {
    ..@spec.Message::new(
      "cb-1",
      "t.sum",
      Json::array([Json::number(1), Json::number(2)]),
    ),
    single_arg: true,
  }
  let parts = body_of(callback.to_celery())
  match parts[0] {
    Array(positional) => {
      assert_eq(positional.length(), 1)
      assert_eq(positional[0], Json::array([Json::number(1), Json::number(2)]))
    }
    _ => fail("the positional arguments are a tuple")
  }
  // and it comes back the same way round, so a callback published here and read
  // there — or the other way — is called with the list it expects.
  let back = @spec.Message::from_celery(callback.to_celery(), queue="celery")
  assert_eq(back.args, Json::array([Json::number(1), Json::number(2)]))
  assert_eq(back.single_arg, true)
}

///|
test "a task with several arguments still travels as those arguments" {
  let message = {
    ..@spec.Message::new(
      "add-1",
      "t.add",
      Json::array([Json::number(2), Json::number(3)]),
    ),
    single_arg: false,
  }
  let parts = body_of(message.to_celery())
  match parts[0] {
    Array(positional) => assert_eq(positional.length(), 2)
    _ => fail("the positional arguments are a tuple")
  }
  let back = @spec.Message::from_celery(message.to_celery(), queue="celery")
  assert_eq(back.args, Json::array([Json::number(2), Json::number(3)]))
  assert_eq(back.single_arg, false)
}

///|
test "a chain travels in the body of its first link" {
  let second = {
    ..@spec.Message::new("run-2", "t.step", Json::number(2)),
    queue: "celery",
    immutable: true,
  }
  let first = {
    ..@spec.Message::new("run-1", "t.step", Json::number(1)),
    chain: [second],
  }
  let embed = embed_of(first.to_celery())
  let chain = match embed.get("chain") {
    Some(Array(items)) => items
    _ => fail("the embed carries the chain")
  }
  assert_eq(chain.length(), 1)
  let link = match chain[0] {
    Object(fields) => fields
    _ => fail("a link is a signature")
  }
  assert_eq(link.get("task"), Some(Json::string("t.step")))
  assert_eq(link.get("immutable"), Some(Json::boolean(true)))
  match link.get("options") {
    Some(Object(options)) => {
      assert_eq(options.get("task_id"), Some(Json::string("run-2")))
      assert_eq(options.get("queue"), Some(Json::string("celery")))
    }
    _ => fail("a link names its id and queue")
  }
}

///|
test "a chain reads back as the messages it carries" {
  let second = {
    ..@spec.Message::new("run-2", "t.plus", Json::number(2)),
    queue: "celery",
    immutable: false,
  }
  let first = {
    ..@spec.Message::new("run-1", "t.plus", Json::number(1)),
    chain: [second],
    immutable: true,
  }
  let back = @spec.Message::from_celery(first.to_celery(), queue="celery")
  assert_eq(back.chain.length(), 1)
  assert_eq(back.chain[0].id, "run-2")
  assert_eq(back.chain[0].name, "t.plus")
  assert_eq(back.chain[0].args, Json::number(2))
  // the flag that decides whether the previous link's result is prepended
  assert_eq(back.chain[0].immutable, false)
}

///|
test "a chord part names its group and carries the callback" {
  let callback = {
    ..@spec.Message::new("cb-7", "t.total", Json::array([])),
    queue: "celery",
    single_arg: true,
  }
  let part = {
    ..@spec.Message::new("part-2", "t.step", Json::number(5)),
    queue: "celery",
    chord: Some({
      key: "group-7",
      index: 2,
      total: 3,
      ids: ["part-1", "part-2", "part-3"],
      callback,
    }),
  }
  let fields = match part.to_celery() {
    Object(fields) => fields
    _ => fail("the envelope is an object")
  }
  let headers = match fields.get("headers") {
    Some(Object(headers)) => headers
    _ => fail("the envelope has headers")
  }
  assert_eq(headers.get("group"), Some(Json::string("group-7")))
  assert_eq(headers.get("group_index"), Some(Json::number(2.0)))
  let callback_json = match embed_of(part.to_celery()).get("chord") {
    Some(Object(fields)) => fields
    _ => fail("the embed carries the callback")
  }
  assert_eq(callback_json.get("task"), Some(Json::string("t.total")))
  // and reading it back gives a chord the worker can join on
  let back = @spec.Message::from_celery(part.to_celery(), queue="celery")
  let chord = match back.chord {
    Some(chord) => chord
    None => fail("the part belongs to a chord")
  }
  assert_eq(chord.key, "group-7")
  assert_eq(chord.index, 2)
  assert_eq(chord.callback.name, "t.total")
  assert_eq(chord.callback.id, "cb-7")
}
