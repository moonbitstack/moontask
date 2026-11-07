// The instants a schedule names inside a window, which is what a backfill walks.

///|
test "an interval counts from the start of the window" {
  let schedule = @moontask.Schedule::Every(interval=1000L)
  assert_eq(schedule.within(0L, 3500L), [0L, 1000L, 2000L, 3000L])
  assert_eq(schedule.within(500L, 2500L), [500L, 1500L])
}

///|
test "a window that holds nothing names nothing" {
  let schedule = @moontask.Schedule::Every(interval=1000L)
  assert_eq(schedule.within(1000L, 1000L), [])
  assert_eq(schedule.within(2000L, 1000L), [])
  assert_eq(@moontask.Schedule::Every(interval=0L).within(0L, 1000L), [])
}

///|
test "a window stops at its limit" {
  let schedule = @moontask.Schedule::Every(interval=1000L)
  assert_eq(schedule.within(0L, 10_000L, limit=3), [0L, 1000L, 2000L])
}

///|
test "a one-off instant is named only inside the window" {
  let schedule = @moontask.Schedule::At(at=1500L)
  assert_eq(schedule.within(0L, 2000L), [1500L])
  assert_eq(schedule.within(0L, 1500L), [])
  assert_eq(schedule.within(2000L, 3000L), [])
}

///|
test "a cron expression is named on its boundaries, the first one included" {
  let expr = @cron.parse("*/10 * * * * *") catch {
    _ => fail("valid expression")
  }
  let schedule = @moontask.Schedule::Cron(expr~)
  // 1_700_000_000_000 is 22:13:20 UTC, which is itself a ten-second boundary, so
  // the window names :20, :30 and :40 and stops before :50.
  assert_eq(schedule.within(1_700_000_000_000L, 1_700_000_025_000L), [
    1_700_000_000_000L, 1_700_000_010_000L, 1_700_000_020_000L,
  ])
  assert_eq(schedule.within(1_700_000_010_000L, 1_700_000_015_000L), [
    1_700_000_010_000L,
  ])
}
