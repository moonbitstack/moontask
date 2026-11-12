// The shapes a fan-out publishes, checked without a worker: what one run per
// item looks like, how a tuple travels, and how a chunk is sliced. What the runs
// then do with each other is `tests/canvas_test.mbt`, which needs a loop.

///|
fn canvas_app() -> @moontask.App {
  let broker = @moontask.MemoryBroker::new(clock=fn() { 1_000_000L })
  let app = @moontask.App::new(broker, backend=broker, counter=broker, clock=fn() {
    1_000_000L
  })
  let _ = broker
  app
}

///|
test "a map is one run per item, each one carrying its item" {
  let app = canvas_app()
  let step = app.task("step", fn(value : Int32, _) { value })
  let sig = step.map([7, 8])
  let links = sig.links()
  assert_eq(links.length(), 2)
  assert_eq(links[0].name, "step")
  assert_eq(links[0].args, Json::number(7.0))
  assert_eq(links[1].args, Json::number(8.0))
}

///|
test "a one-element tuple of a starmap travels as one argument" {
  let app = canvas_app()
  let step = app.task("step", fn(value : Int32, _) { value })
  let one = step.starmap([[5]]).links()
  assert_eq(one[0].args, Json::number(5.0))
  // The flag is what makes the envelope wrap it once and not twice.
  assert_eq(one[0].single_arg, true)
}

///|
test "a tuple of several arguments travels as the argument list" {
  let app = canvas_app()
  let pair = app.task("pair", fn(values : Array[Int32], _) { values.length() })
  let links = pair.starmap([[1, 2], [3]]).links()
  assert_eq(links[0].args.stringify(), "[1,2]")
  assert_eq(links[0].single_arg, false)
  assert_eq(links[1].args, Json::number(3.0))
  assert_eq(links[1].single_arg, true)
}

///|
test "a starmap refuses a tuple that is not a list" {
  let app = canvas_app()
  let step = app.task("step", fn(value : Int32, _) { value })
  let refused = try {
    let _ = step.starmap([Json::object({ "a": Json::number(1.0) })])
    false
  } catch {
    @spec.Refused::Malformed(got~) => got.contains("not a list")
    _ => false
  }
  assert_true(refused)
}

///|
/// The tuples one chunk message carries.
fn chunk_items(link : @spec.Message) -> String raise @spec.Refused {
  match link.args {
    Object(fields) =>
      match fields.get("items") {
        Some(items) => items.stringify()
        None => raise @spec.Refused::Malformed(got="a chunk with no items")
      }
    _ => raise @spec.Refused::Malformed(got="a chunk that is not an object")
  }
}

///|
test "a chunk is a part per slice, the last one short" {
  let app = canvas_app()
  let step = app.task("step", fn(value : Int32, _) { value })
  let links = step.chunks([[1], [2], [3], [4], [5]], size=2).links()
  assert_eq(links.length(), 3)
  for link in links {
    assert_eq(link.name, @moontask.chunk_name)
    assert_eq(link.queue, "default")
  }
  assert_eq(chunk_items(links[0]), "[[1],[2]]")
  assert_eq(chunk_items(links[1]), "[[3],[4]]")
  assert_eq(chunk_items(links[2]), "[[5]]")
}

///|
test "a chunk of a size below one is refused" {
  let app = canvas_app()
  let step = app.task("step", fn(value : Int32, _) { value })
  let refused = try {
    let _ = step.chunks([[1]], size=0)
    false
  } catch {
    @spec.Refused::Malformed(got~) => got.contains("chunk of 0")
    _ => false
  }
  assert_true(refused)
}

///|
test "a fan-out of nothing publishes nothing" {
  let app = canvas_app()
  let step = app.task("step", fn(value : Int32, _) { value })
  let nothing : Array[Int32] = []
  assert_eq(step.map(nothing).links().length(), 0)
  assert_eq(step.starmap(nothing).links().length(), 0)
  assert_eq(step.chunks(nothing, size=3).links().length(), 0)
}
