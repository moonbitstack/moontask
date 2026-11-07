// What a run record says, independent of any worker.

///|
test "a status writes the label a table keeps and reads it back" {
  assert_eq(@flow.Status::Succeeded.code(), "SUCCESS")
  assert_eq(@flow.Status::Failed(got="boom").code(), "FAILURE")
  assert_eq(@flow.Status::of("SUCCESS"), @flow.Status::Succeeded)
  assert_eq(
    @flow.Status::of("FAILURE", got="boom"),
    @flow.Status::Failed(got="boom"),
  )
}

///|
test "a run answers the value of a step it reached and nothing for one it did not" {
  let run : @flow.Run = {
    flow: "etl",
    thread: "etl:1000",
    at: 1000L,
    state: { "extract": { "rows": 3 } },
    status: @flow.Status::Succeeded,
  }
  assert_eq(run.output_of("extract").unwrap().stringify(), "{\"rows\":3}")
  assert_eq(run.output_of("load") is None, true)
  assert_true(run.ok())
  assert_eq(run.to_string(), "run etl:1000: succeeded")
}

///|
test "a run whose state is not an object answers nothing" {
  let run : @flow.Run = {
    flow: "odd",
    thread: "odd:0",
    at: 0L,
    state: Json::null(),
    status: @flow.Status::Failed(got="stopped before the first step"),
  }
  assert_eq(run.output_of("a") is None, true)
  assert_false(run.ok())
  assert_eq(run.to_string(), "run odd:0: failed, stopped before the first step")
}
