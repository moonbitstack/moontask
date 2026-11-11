// Copyright 2026 Leo Cheng
// SPDX-License-Identifier: Apache-2.0

///|
/// `#task.def` on a free function, the counterpart of Celery's `@app.task`: a `Tasks` record
/// with one typed handle per annotated function, and `register_tasks`, which registers them all on
/// an app and hands the record back. The functions themselves are not touched, since a generator
/// can only add code.
priv struct Annotated {}

///|
impl @gen.Generator for Annotated with fn name(_) {
  "moontask tasks"
}

///|
/// The options are the fields of `Policy` a task usually sets; the rest are reached by calling
/// `App::task` directly.
let task_spec : @attr.Spec = @attr.Spec::new("task.def")
  .optional("name", Str)
  .optional("queue", Str)
  .optional("retries", Str)
  .optional("timeout", Str)
  .optional("acks_late", Bool)
  .optional("ignore_result", Bool)

///|
fn at(attr : @attribute.Attribute, file : String) -> String {
  "\{file}:\{attr.loc.start.lnum}:\{attr.loc.start.column()}"
}

///|
/// A duration such as `250ms`, `30s`, `5m` or `1h`, in the milliseconds the app's clock runs in.
fn millis(text : String, where_ : String) -> Int64 raise {
  let (digits, scale) = if text.has_suffix("ms") {
    (text.exact_view(end=text.length() - 2), 1L)
  } else if text.has_suffix("s") {
    (text.exact_view(end=text.length() - 1), 1000L)
  } else if text.has_suffix("m") {
    (text.exact_view(end=text.length() - 1), 60000L)
  } else if text.has_suffix("h") {
    (text.exact_view(end=text.length() - 1), 3600000L)
  } else {
    fail(
      "\{where_}: `\{text}` is not a duration: write `250ms`, `30s`, `5m` or `1h`",
    )
  }
  let number = @string.parse_int64(digits.to_owned()) catch {
    _ => fail("\{where_}: `\{text}` is not a duration")
  }
  if number <= 0L {
    fail("\{where_}: `\{text}` is not a positive duration")
  }
  number * scale
}

///|
/// The `policy=` argument for the options the annotation gives, or nothing when it gives none, so
/// the app's preset applies unchanged.
fn policy_of(args : @attr.Args, where_ : String) -> String raise {
  let fields = []
  if args.str("queue") is Some(queue) {
    if queue.is_empty() {
      fail("\{where_}: `queue` names a queue")
    }
    fields.push("queue: \{@quote.string(queue)}")
  }
  if args.str("retries") is Some(retries) {
    guard retries is [_, ..] && retries.iter().all(c => c is ('0'..<='9')) else {
      fail("\{where_}: `retries` is a count, not `\{retries}`")
    }
    fields.push("retries: \{retries}")
  }
  if args.str("timeout") is Some(text) {
    fields.push("timeout: Some(\{millis(text, where_)}L)")
  }
  if args.bool("acks_late") is Some(late) {
    fields.push("acks_late: \{late}")
  }
  if args.bool("ignore_result") is Some(ignore) {
    fields.push("ignore_result: \{ignore}")
  }
  if fields.is_empty() {
    ""
  } else {
    // The type is named: a bare `{ ..preset }` after `policy=` is printed as `::{` and then
    // misread as more positional arguments.
    ", policy=@moontask.Policy::{ ..@moontask.policy, \{fields.join(", ")} }"
  }
}

///|
/// One annotated function: the field of `Tasks` it becomes and the registration that fills it.
priv struct Entry {
  field : String
  register : String
}

///|
fn entry(
  note : @attribute.Attribute,
  name : String,
  params : Array[@gen.Param],
  returns : String?,
  is_async : Bool,
  file : String,
) -> (String, Entry) raise {
  let where_ = at(note, file)
  let args = task_spec.bind(note)
  // A plain function is a task as it stands, as in Celery: the worker takes a plain closure where
  // it takes an async one, so only an `async fn` gets an `async` closure.
  // A trailing `Context` is the run's own record (attempt, queue, clock); the parameter before it,
  // if any, is the one JSON value a message carries.
  let (plain, context) = match params {
    [.. rest, last] if last.ty is Some(ty) && ty.has_suffix("Context") =>
      (rest.to_owned(), true)
    _ => (params, false)
  }
  let argument = match plain {
    [] => None
    [one] => {
      guard one.name is Some(_) && one.ty is Some(ty) && one.pass is Positional else {
        fail(
          "\{where_}: a task's argument is a plain parameter, decoded from the message",
        )
      }
      Some(ty)
    }
    _ =>
      fail(
        "\{where_}: a task takes one argument, the one JSON value its message carries; put several values in a record",
      )
  }
  let task_name = args.str("name").unwrap_or(name)
  if task_name.is_empty() {
    fail("\{where_}: `name` names the task")
  }
  let a = argument.unwrap_or("Unit")
  let r = returns.unwrap_or("Unit")
  let call = match (argument, context) {
    (Some(_), true) => "\{name}(arg, ctx)"
    (Some(_), false) => "\{name}(arg)"
    (None, true) => "\{name}(ctx)"
    (None, false) => "\{name}()"
  }
  let (arg_name, ctx_name) = (
    if argument is Some(_) {
      "arg"
    } else {
      "_"
    },
    if context {
      "ctx"
    } else {
      "_"
    },
  )
  // An arrow takes the raising of the function it calls; an `async fn` closure has to be written
  // out, since an arrow cannot be async.
  let params = "(\{arg_name} : \{a}, \{ctx_name} : @moontask.Context)"
  let closure = if is_async {
    "async fn\{params} { \{call} }"
  } else {
    "\{params} => \{call}"
  }
  let register =
    $|\{name}: app.task(
    $|  \{@quote.string(task_name)},
    $|  \{closure}\{policy_of(args, where_)},
    $|),
  (task_name, { field: "\{name} : @moontask.Task[\{a}, \{r}]", register, })
}

///|
impl @gen.Generator for Annotated with fn generate(_, decls) {
  let entries = []
  let names : Map[String, String] = Map([])
  let mut all_public = true
  for decl in decls {
    match decl {
      Func(
        owner~,
        name~,
        params~,
        returns~,
        is_async~,
        attrs~,
        public~,
        file~,
        ..
      ) => {
        for attr in @attr.malformed(attrs, "task") {
          fail(
            "\{at(attr, file)}: `\{attr.raw}` does not parse as an annotation",
          )
        }
        let found = @attr.under(attrs, "task")
        guard found is [note, ..] else { continue }
        for attr in found {
          if @attr.name_of(attr) != Some("task.def") {
            fail(
              "\{at(attr, file)}: `\{attr.raw}` is not a moontask annotation; a task is declared with `#task.def`",
            )
          }
        }
        if found.length() > 1 {
          fail("\{at(found[1], file)}: `#task.def` is written twice")
        }
        if owner is Some(_) {
          fail("\{at(note, file)}: `#task.def` goes on a free function")
        }
        let (task_name, made) = entry(
          note, name, params, returns, is_async, file,
        )
        if names.get(task_name) is Some(other) {
          fail(
            "\{at(note, file)}: the task `\{task_name}` is already `\{other}`",
          )
        }
        names[task_name] = name
        all_public = all_public && public
        entries.push(made)
      }
      Struct(attrs~, file~, ..) | Enum(attrs~, file~, ..) =>
        if @attr.under(attrs, "task") is [first, ..] {
          fail("\{at(first, file)}: `#task.def` goes on a free function")
        }
      Other(..) => ()
    }
  }
  guard entries is [_, ..] else { return @quote.items("").render() }
  // The record and the function are public only when every task is: a public record whose field
  // names a private argument type does not compile.
  let vis = if all_public { "pub " } else { "" }
  let fields = entries.map(e => e.field).join("\n  ")
  let registers = entries.map(e => e.register).join("\n")
  let record =
    $|/// The tasks this package declares with `#task.def`, as typed handles on the app they were
    $|/// registered on.
    $|\{vis}struct Tasks {
    $|  \{fields}
    $|}
  let register =
    $|/// Register every `#task.def` of this package on `app`, under its name and policy, and return the
    $|/// handles. A worker calls it to run the tasks; a producer calls it to send them.
    $|\{vis}fn register_tasks(app : @moontask.App) -> Tasks {
    $|  {
    $|\{registers}
    $|  }
    $|}
  @quote.items(record + "\n\n" + register).render()
}
