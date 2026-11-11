// Copyright 2026 Leo Cheng
// SPDX-License-Identifier: Apache-2.0

///|
fn flat(code : String) -> String {
  let out = StringBuilder()
  for c in code {
    if c != ' ' && c != '\n' {
      out.write_char(c)
    }
  }
  out.to_string().replace_all(old=",)", new=")").replace_all(old=",]", new="]")
}

///|
fn generate(source : String) -> String raise {
  flat(@gen.render(Annotated::{ }, [("m.mbt", source)]))
}

///|
fn refused(source : String) -> String {
  try {
    ignore(generate(source))
    ""
  } catch {
    e => "\{e}"
  }
}

///|
test "a task with an argument and the run's context is registered under its own name" {
  let code = generate(
    "#task.def\npub async fn place(order : Order, ctx : @moontask.Context) -> String {\n  order.id\n}",
  )
  assert_true(
    code.contains(
      flat("pub struct Tasks {\n  place : @moontask.Task[Order, String]\n}"),
    ),
  )
  assert_true(
    code.contains(flat("pub fn register_tasks(app : @moontask.App) -> Tasks")),
  )
  assert_true(
    code.contains(
      flat(
        "place: app.task(\"place\", async fn(arg : Order, ctx : @moontask.Context) { place(arg, ctx) })",
      ),
    ),
  )
}

///|
test "no argument is a unit message, and no context is left out of the call" {
  let code = generate("#task.def\npub async fn tick() -> Unit {\n  ()\n}")
  assert_true(code.contains(flat("tick : @moontask.Task[Unit, Unit]")))
  assert_true(
    code.contains(flat("async fn(_ : Unit, _ : @moontask.Context) { tick() }")),
  )
  let code = generate(
    "#task.def\npub async fn shout(text : String) -> String {\n  text\n}",
  )
  assert_true(
    code.contains(
      flat("async fn(arg : String, _ : @moontask.Context) { shout(arg) }"),
    ),
  )
}

///|
test "the options become a policy over the app's preset" {
  let code = generate(
    "#task.def(name=\"orders.place\", queue=\"orders\", retries=\"3\", timeout=\"30s\", acks_late=true, ignore_result=false)\npub async fn place(order : Order) -> String {\n  order.id\n}",
  )
  assert_true(code.contains(flat("app.task(\"orders.place\",")))
  assert_true(
    code.contains(
      flat(
        "policy=@moontask.Policy::{ ..@moontask.policy, queue: \"orders\", retries: 3, timeout: Some(30000L), acks_late: true, ignore_result: false, }",
      ),
    ),
  )
  // Without options the app's preset applies as it stands.
  let plain = generate("#task.def\npub async fn tick() -> Unit {\n  ()\n}")
  assert_false(plain.contains("policy="))
}

///|
test "a private task keeps the record and the function private" {
  let code = generate(
    "#task.def\nasync fn place(order : Order) -> String {\n  order.id\n}\n\n#task.def\npub async fn tick() -> Unit {\n  ()\n}",
  )
  assert_false(code.contains(flat("pub struct Tasks")))
  assert_true(code.contains(flat("struct Tasks")))
  assert_false(code.contains(flat("pub fn register_tasks")))
  assert_true(code.contains(flat("tick : @moontask.Task[Unit, Unit]")))
}

///|
test "nothing annotated, nothing written" {
  let code = generate("pub async fn tick() -> Unit {\n  ()\n}")
  assert_false(code.contains("Tasks"))
  assert_false(code.contains("register_tasks"))
}

///|
test "a plain function is a task too, wrapped in the closure the worker runs" {
  let code = generate("#task.def\npub fn double(n : Int) -> Int {\n  n * 2\n}")
  assert_true(code.contains(flat("double : @moontask.Task[Int, Int]")))
  assert_true(
    code.contains(flat("(arg : Int, _ : @moontask.Context) => double(arg)")),
  )
}

///|
test "what a task cannot be is refused with the place it is written" {
  assert_true(
    refused("#task.def\npub async fn f(a : Int, b : Int) -> Int {\n  a\n}").contains(
      "m.mbt:1:1: a task takes one argument",
    ),
  )
  assert_true(
    refused("#task.def\npub async fn f(a : Int, b : Int) -> Int {\n  a\n}").contains(
      "a task takes one argument",
    ),
  )
  assert_true(
    refused("#task.def\npub async fn f(a~ : Int) -> Int {\n  a\n}").contains(
      "a task's argument is a plain parameter",
    ),
  )
  assert_true(
    refused(
      "#task.def(name=\"x\")\npub async fn f() -> Unit {\n  ()\n}\n\n#task.def(name=\"x\")\npub async fn g() -> Unit {\n  ()\n}",
    ).contains("the task `x` is already `f`"),
  )
  assert_true(
    refused("#task.def(retries=\"three\")\npub async fn f() -> Unit {\n  ()\n}").contains(
      "`retries` is a count",
    ),
  )
  assert_true(
    refused("#task.def(timeout=\"10\")\npub async fn f() -> Unit {\n  ()\n}").contains(
      "is not a duration",
    ),
  )
  assert_true(
    refused("#task.def\n#task.def\npub async fn f() -> Unit {\n  ()\n}").contains(
      "`#task.def` is written twice",
    ),
  )
  assert_true(
    refused("#task.run\npub async fn f() -> Unit {\n  ()\n}").contains(
      "is not a moontask annotation",
    ),
  )
  assert_true(
    refused("#task.def\npub struct S {\n  a : Int\n}").contains(
      "`#task.def` goes on a free function",
    ),
  )
}
