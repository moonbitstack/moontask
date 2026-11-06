// The rhythm itself: what a chain builds, and where the next run lands.

///|
test "a unit scales the interval" {
  assert_eq(@schedule.every(1).seconds().interval(), 1000L)
  assert_eq(@schedule.every(2).minutes().interval(), 120_000L)
  assert_eq(@schedule.every(3).hours().interval(), 10_800_000L)
  assert_eq(@schedule.every(1).days().interval(), 86_400_000L)
  assert_eq(@schedule.every(250).interval(), 250L)
}

///|
test "an offset inside the period is read as a clock time" {
  let per_minute = @schedule.every(1).minutes()
  let job = per_minute.at(":30") catch {
    _ => fail("thirty seconds fits a minute")
  }
  // 2023-11-14T22:13:20Z; the next half minute is 22:13:30.
  assert_eq(job.next_after(1_700_000_000_000L), 1_700_000_010_000L)
  // And then once a minute after that.
  assert_eq(job.next_after(1_700_000_010_000L), 1_700_000_070_000L)
}

///|
test "a two-field offset lands inside the hour" {
  let hourly = @schedule.every(1).hours()
  let job = hourly.at("15:30") catch {
    _ => fail("fifteen minutes fits an hour")
  }
  // 22:13:20 → 22:15:30, which is still ahead of it.
  assert_eq(job.next_after(1_700_000_000_000L), 1_700_000_130_000L)
}

///|
test "a three-field offset lands inside the day" {
  let daily = @schedule.every(1).days()
  let job = daily.at("9:15:00") catch { _ => fail("nine hours fits a day") }
  // 22:13:20 → the next day at 09:15:00.
  assert_eq(job.next_after(1_700_000_000_000L), 1_700_039_700_000L)
}

///|
test "an offset that does not fit the period is refused" {
  let too_long = try {
    let _ = @schedule.every(10).at(":30")
    false
  } catch {
    @spec.Refused::Malformed(_) => true
    _ => false
  }
  assert_true(too_long)
}

///|
test "an offset that is not a clock time is refused" {
  let junk = try {
    let _ = @schedule.every(1).hours().at("soon")
    false
  } catch {
    @spec.Refused::Malformed(_) => true
    _ => false
  }
  assert_true(junk)
  let negative = try {
    let _ = @schedule.every(1).hours().at("-1:00")
    false
  } catch {
    @spec.Refused::Malformed(_) => true
    _ => false
  }
  assert_true(negative)
}

///|
test "the next run keeps the phase" {
  let job = @schedule.every(10).minutes()
  // Added at 22:13:20; the rhythm is 22:20:00, not 22:23:20.
  assert_eq(job.next_after(1_700_000_000_000L), 1_700_000_400_000L)
  // Even when the loop wakes late, the phase is kept: the run after 22:20:00 is
  // 22:30:00, not ten minutes after the wake.
  assert_eq(job.next_after(1_700_000_400_000L), 1_700_001_000_000L)
}

///|
test "a job starts with no name and no runs" {
  let job = @schedule.every(1).seconds().named("cleanup")
  assert_eq(job.name(), "cleanup")
  assert_eq(job.runs(), 0)
  assert_eq(job.failures(), 0)
}
