// When a schedule fires: the anchor of an interval, a one-off instant, and the
// boundaries a cron expression names.

///|
test "an interval schedule is anchored to the time it was added" {
  let every = @moontask.Schedule::Every(interval=60000L)
  assert_eq(every.first(1_000_000L), 1_060_000L)
  assert_eq(every.after(1_060_000L), Some(1_120_000L))
}

///|
test "an instant schedule fires once" {
  let at = @moontask.Schedule::At(at=1_000_500L)
  assert_eq(at.first(1_000_000L), 1_000_500L)
  assert_eq(at.after(1_000_500L), None)
}

///|
test "an instant already past fires on the first tick" {
  let at = @moontask.Schedule::At(at=900_000L)
  assert_eq(at.first(1_000_000L), 1_000_000L)
  assert_eq(at.after(1_000_000L), None)
}

///|
test "a cron schedule names second boundaries" {
  // 2023-11-14T22:13:20Z.
  let expr = @cron.parse("*/10 * * * * *") catch {
    _ => fail("the expression is valid")
  }
  let schedule = @moontask.Schedule::Cron(expr~)
  assert_eq(schedule.first(1_700_000_000_000L), 1_700_000_010_000L)
  assert_eq(schedule.after(1_700_000_010_000L), Some(1_700_000_020_000L))
}

///|
test "a five-field cron schedule names minute boundaries" {
  let expr = @cron.parse("*/5 * * * *") catch {
    _ => fail("the expression is valid")
  }
  let schedule = @moontask.Schedule::Cron(expr~)
  // 22:13:20 is not a boundary; the next one is 22:15:00.
  assert_eq(schedule.first(1_700_000_000_000L), 1_700_000_100_000L)
}

///|
test "a cron schedule pinned to a day fires on that day only" {
  let expr = @cron.parse("0 0 0 1 * *") catch {
    _ => fail("the expression is valid")
  }
  let schedule = @moontask.Schedule::Cron(expr~)
  // 2023-11-14. The next first-of-month at midnight is 2023-12-01T00:00:00Z.
  assert_eq(schedule.first(1_700_000_000_000L), 1_701_388_800_000L)
}

///|
test "milliseconds and instants convert both ways" {
  let at = 1_700_000_000_123L
  match @moontask.moment_of(at) {
    Some(moment) => assert_eq(@moontask.milliseconds_of(moment), Some(at))
    None => fail("the instant is in range")
  }
}
