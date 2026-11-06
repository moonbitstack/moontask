// The wire form: every message a client writes must read back as the same
// message, and every shape a producer is known to write must be readable.

///|
test "a message round-trips through its text" {
  let message = @spec.Message::new("t-1", "mail.send", Json::string("hello"))
  let text = message.text()
  let back = @spec.Message::parse(text)
  assert_eq(back, message)
  assert_eq(back.args, Json::string("hello"))
}

///|
test "a scheduled message keeps its instants to the millisecond" {
  let message = {
    ..@spec.Message::new("t-2", "mail.send", Json::null()),
    eta: Some(1_700_000_000_123L),
    expires: Some(1_700_000_600_456L),
  }
  let back = @spec.Message::parse(message.text())
  assert_eq(back.eta, Some(1_700_000_000_123L))
  assert_eq(back.expires, Some(1_700_000_600_456L))
}

///|
test "an instant before the epoch survives the round trip" {
  let message = {
    ..@spec.Message::new("t-3", "mail.send", Json::null()),
    eta: Some(-1500L),
  }
  let back = @spec.Message::parse(message.text())
  assert_eq(back.eta, Some(-1500L))
}

///|
test "the keys are the protocol's" {
  let json = Json({
    ..@spec.Message::new("t-4", "mail.send", Json::array([Json::number(1.0)])),
    queue: "mail",
    priority: 3,
    retries: 2,
  })
  let text = json.stringify()
  assert_true(text.contains("\"task\":\"mail.send\""))
  assert_true(text.contains("\"retries\":2"))
  assert_true(text.contains("\"queue\":\"mail\""))
  assert_true(text.contains("\"utc\":true"))
}

///|
test "a message without the optional keys is readable" {
  let text = "{\"id\":\"t-5\",\"task\":\"mail.send\",\"args\":[]}"
  let message = @spec.Message::parse(text)
  assert_eq(message.id, "t-5")
  assert_eq(message.queue, "default")
  assert_eq(message.priority, 0)
  assert_eq(message.retries, 0)
  assert_eq(message.eta, None)
  assert_eq(message.chain.length(), 0)
}

///|
test "a chain travels inside the message" {
  let tail = @spec.Message::new("t-7", "mail.send", Json::string("b"))
  let head = {
    ..@spec.Message::new("t-6", "mail.send", Json::string("a")),
    chain: [tail],
  }
  let back = @spec.Message::parse(head.text())
  assert_eq(back.chain.length(), 1)
  assert_eq(back.chain[0].id, "t-7")
  assert_eq(back.chain[0].args, Json::string("b"))
}

///|
test "a chord travels inside the message" {
  let callback = @spec.Message::new("t-9", "report.build", Json::null())
  let part = {
    ..@spec.Message::new("t-8", "mail.send", Json::null()),
    chord: Some({
      key: "chord:t-8",
      index: 1,
      total: 2,
      ids: ["t-8", "t-8b"],
      callback,
    }),
  }
  let back = @spec.Message::parse(part.text())
  match back.chord {
    Some(chord) => {
      assert_eq(chord.key, "chord:t-8")
      assert_eq(chord.index, 1)
      assert_eq(chord.total, 2)
      assert_eq(chord.ids.length(), 2)
      assert_eq(chord.callback.name, "report.build")
    }
    None => fail("the chord was lost")
  }
}

///|
test "text that is not a message is refused, not guessed" {
  let refused = try {
    let _ = @spec.Message::parse("{\"id\":\"t-10\"}")
    false
  } catch {
    @spec.Refused::Malformed(_) => true
    _ => false
  }
  assert_true(refused)
}

///|
test "text that is not json at all is refused" {
  let refused = try {
    let _ = @spec.Message::parse("not json")
    false
  } catch {
    @spec.Refused::Malformed(_) => true
    _ => false
  }
  assert_true(refused)
}

///|
test "a state keeps its label across a store" {
  let states : Array[@spec.State] = [
    @spec.Pending,
    @spec.Received,
    @spec.Started,
    @spec.Succeeded,
    @spec.Failed,
    @spec.Retrying,
    @spec.Revoked,
  ]
  for state in states {
    assert_eq(@spec.State::of(state.code()), state)
  }
  let refused = try {
    let _ = @spec.State::of("WORKING")
    false
  } catch {
    @spec.Refused::Unknown(_) => true
    _ => false
  }
  assert_true(refused)
}

///|
test "an outcome round-trips with its value" {
  let outcome = @spec.outcome_succeeded(
    Json::array([Json::number(1.0)]),
    2,
    1_700_000_000_000L,
  )
  let back : @spec.Outcome = @json.from_json(Json(outcome)) catch {
    _ => fail("the outcome did not decode")
  }
  assert_eq(back, outcome)
}

///|
test "an outcome round-trips with its error" {
  let outcome = @spec.outcome_failed("boom", 3, 1_700_000_000_000L)
  let back : @spec.Outcome = @json.from_json(Json(outcome)) catch {
    _ => fail("the outcome did not decode")
  }
  assert_eq(back.state, @spec.Failed)
  assert_eq(back.error, Some("boom"))
  assert_eq(back.retries, 3)
}
