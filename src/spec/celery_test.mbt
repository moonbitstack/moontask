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
  let outcome = @spec.outcome_succeeded(Json::string("done"), 0, 1_700_000_000_000L)
  let back = @spec.Outcome::from_celery(outcome.to_celery("run-9"))
  assert_eq(back.state, @spec.Succeeded)
  assert_eq(back.result, Some(Json::string("done")))
}
