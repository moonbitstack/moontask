// The policy: how many attempts, how long apart, and how the spread behaves.

///|
test "the default policy holds still until asked otherwise" {
  assert_eq(@moontask.policy.retries, 0)
  assert_eq(@moontask.policy.queue, "default")
  assert_false(@moontask.policy.acks_late)
  assert_false(@moontask.policy.ignore_result)
  assert_eq(@moontask.policy.timeout, None)
  assert_eq(@moontask.policy.rate, None)
}

///|
test "the first attempt never waits" {
  for
    backoff in [
      @moontask.Backoff::Fixed(delay=1000L),
      @moontask.Backoff::Exponential(base=1000L, cap=600000L, jitter=true),
    ] {
    assert_eq(backoff.delay(0, "t"), 0L)
  }
}

///|
test "a fixed backoff waits the same every time" {
  let backoff = @moontask.Backoff::Fixed(delay=2500L)
  assert_eq(backoff.delay(1, "t"), 2500L)
  assert_eq(backoff.delay(2, "t"), 2500L)
  assert_eq(backoff.delay(9, "t"), 2500L)
}

///|
test "an exponential backoff doubles and stops at the cap" {
  let backoff = @moontask.Backoff::Exponential(
    base=1000L,
    cap=10000L,
    jitter=false,
  )
  assert_eq(backoff.delay(1, "t"), 1000L)
  assert_eq(backoff.delay(2, "t"), 2000L)
  assert_eq(backoff.delay(3, "t"), 4000L)
  assert_eq(backoff.delay(4, "t"), 8000L)
  assert_eq(backoff.delay(5, "t"), 10000L)
  assert_eq(backoff.delay(50, "t"), 10000L)
}

///|
test "a jittered backoff stays within the interval and repeats itself" {
  let backoff = @moontask.Backoff::Exponential(
    base=1000L,
    cap=60000L,
    jitter=true,
  )
  let mut attempt = 1
  while attempt <= 12 {
    let wait = backoff.delay(attempt, "task-1")
    let plain = @moontask.Backoff::Exponential(
      base=1000L,
      cap=60000L,
      jitter=false,
    ).delay(attempt, "task-1")
    assert_true(wait >= 1L)
    assert_true(wait <= plain)
    // The same task retried again waits the same, on any machine.
    assert_eq(wait, backoff.delay(attempt, "task-1"))
    attempt = attempt + 1
  }
}

///|
test "a jittered backoff spreads different tasks apart" {
  let backoff = @moontask.Backoff::Exponential(
    base=60000L,
    cap=60000L,
    jitter=true,
  )
  let waits = []
  for name in ["a", "b", "c", "d", "e", "f", "g", "h"] {
    waits.push(backoff.delay(3, name))
  }
  let distinct = []
  for wait in waits {
    if !distinct.contains(wait) {
      distinct.push(wait)
    }
  }
  assert_true(distinct.length() > 1)
}

///|
test "a policy answers the wait for an attempt" {
  let custom = {
    ..@moontask.policy,
    backoff: @moontask.Backoff::Fixed(delay=42L),
  }
  assert_eq(custom.backoff_delay(3, "task"), 42L)
  assert_eq(@moontask.policy.backoff_delay(1, "task") > 0L, true)
}
