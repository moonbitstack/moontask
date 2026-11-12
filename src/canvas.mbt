// The canvas: chain, group and chord, built out of the messages themselves.
//
// A chain is a message that carries its own tail, a group is messages sent
// together, and a chord is a group whose parts each carry the same counter key
// and the callback waiting behind it. No coordinator is involved: whichever
// worker finishes the last part sees the counter reach zero and sends the
// callback, so any worker can be the one that advances the canvas.

///|
/// Whether a sig waits on one run or on all of them.
#derive(Eq, Debug)
pub(all) enum Shape {
  One
  All
}

///|
pub extend Shape with Eq::{not_equal, equal}

///|
pub extend Shape with Debug::{to_repr}

///|
/// A piece of work that has not been published yet.
///
/// The messages are ready to go; `waits` is what a caller waits on afterwards,
/// which for a chain is its last link and for a chord is the callback. Building
/// a sig publishes nothing, so a canvas can be built once and sent twice.
pub struct Sig {
  publish : Array[@spec.Message]
  waits : Array[String]
  shape : Shape
}

///|
/// A sig of the single message `messages[0]`.
fn Sig::of(messages : Array[@spec.Message]) -> Sig {
  {
    publish: messages,
    waits: messages.map(fn(message) { message.id }),
    shape: One,
  }
}

///|
/// Every message this sig stands for, in the order they run: the links of a
/// chain, the parts of a group.
pub fn Sig::links(self : Sig) -> Array[@spec.Message] {
  let out = []
  for message in self.publish {
    out.push({ ..message, chain: [], })
    for link in message.chain {
      out.push({ ..link, chain: [], })
    }
  }
  out
}

///|
/// The runs in `sigs`, in order, each one started when the one before it
/// succeeds.
///
/// A chain of chains is flattened, so `chain([a, chain([b, c])])` is `a → b → c`.
/// Building a chain publishes nothing.
pub fn chain(sigs : Array[Sig]) -> Sig {
  let messages = []
  for sig in sigs {
    for message in sig.links() {
      // Every link of a chain built here is called with the arguments it was
      // given, which is what `immutable` means to a worker on either side; the
      // flag has to be on each link, since each one is published on its own.
      messages.push({ ..message, immutable: true, })
    }
  }
  if messages.length() == 0 {
    return { publish: [], waits: [], shape: One, }
  }
  let tail = []
  let mut at = 1
  while at < messages.length() {
    tail.push(messages[at])
    at = at + 1
  }
  let head = { ..messages[0], chain: tail, }
  { publish: [head], waits: [messages[messages.length() - 1].id], shape: One, }
}

///|
/// Every run in `sigs`, all at once, waited on together.
///
/// A chain given to a group is not broken up: its head is published and its tail
/// travels with it, so the group waits on the chain's last link. The handle's
/// value is the list of results in the order the sigs were given.
pub fn group(sigs : Array[Sig]) -> Sig {
  let publish = []
  let waits = []
  for sig in sigs {
    for message in sig.publish {
      publish.push(message)
    }
    for id in sig.waits {
      waits.push(id)
    }
  }
  { publish, waits, shape: All, }
}

///|
/// The runs in `parts`, then `callback` once every part has succeeded.
///
/// The callback is sent with the list of part results as its argument, in the
/// order the parts were given. Every part carries the same key and the same
/// list of ids, so the part that finishes last can collect the set by itself.
///
/// A part that fails for good does not hold the chord open: the last part to
/// finish records the callback as failed, so nobody waits on a callback that
/// will never be sent.
pub fn chord(parts : Array[Sig], callback : Sig) -> Sig {
  let messages = []
  for sig in parts {
    for message in sig.publish {
      messages.push(message)
    }
  }
  if messages.length() == 0 || callback.publish.length() == 0 {
    return { publish: [], waits: callback.waits, shape: One, }
  }
  let callback_message = callback.publish[0]
  let key = "chord:" + callback_message.id
  let ids = messages.map(fn(message) { message.id })
  let total = messages.length()
  let publish = []
  let mut index = 0
  for message in messages {
    publish.push({
      ..message,
      chord: Some({ key, index, total, ids, callback: callback_message, }),
    })
    index = index + 1
  }
  { publish, waits: callback.waits, shape: One, }
}

///|
/// One run of this task per item, all at once: a group whose parts are the items
/// in the order they were given.
///
/// Each item is the whole argument of its run, so a task that takes a record is
/// mapped over records. The handle answers the results in that same order, and
/// an empty list is a group with nothing to wait for, which answers an empty
/// list rather than refusing.
pub fn[A : ToJson, R] Task::map(self : Task[A, R], items : Array[A]) -> Sig {
  let sigs = []
  for item in items {
    sigs.push(self.sig(item))
  }
  group(sigs)
}

///|
/// The argument one tuple of a fan-out hands a body.
///
/// A tuple is a list of arguments, and a body here takes one value: a tuple of
/// one is that value, a longer tuple is the list itself, and an empty tuple is
/// the empty value. A consumer on the other side of a shared queue reads a list
/// of one the same way, so both sides run the same run.
fn tuple_value(tuple : Json) -> (Json, Bool) raise @spec.Refused {
  match tuple {
    Array(items) =>
      match items {
        [] => (Json::null(), false)
        [only] => (only, true)
        many => (Json::array(many), false)
      }
    _ =>
      raise @spec.Refused::Malformed(
        got="a tuple that is not a list: " + tuple.stringify(),
      )
  }
}

///|
/// The message one tuple of a fan-out becomes.
fn[A, R, B : ToJson] Task::tuple_message(
  self : Task[A, R],
  tuple : B,
) -> @spec.Message raise @spec.Refused {
  let (args, single) = tuple_value(Json(tuple))
  { ..self.message(args), single_arg: single, }
}

///|
/// One run per tuple, all at once, each tuple spread across the run's arguments.
///
/// This is the shape a fan-out takes when the runs take several arguments: a
/// list per run, published as that run's argument list. The handle answers the
/// results in the order the tuples were given.
pub fn[A, R, B : ToJson] Task::starmap(
  self : Task[A, R],
  tuples : Array[B],
) -> Sig raise @spec.Refused {
  let sigs = []
  for tuple in tuples {
    sigs.push(Sig::of([self.tuple_message(tuple)]))
  }
  group(sigs)
}

///|
/// The name a chunk runs under.
///
/// One body serves every worker, so a chunk published by one process is run by a
/// worker in another: the message names the task the chunk is of, and the body
/// that runs it comes with the worker rather than from whoever built the canvas.
pub let chunk_name : String = "moontask.chunks"

///|
/// The runs of `tuples`, `size` of them per run, all at once.
///
/// A part of the group is one run of the chunk task, which runs its slice of the
/// tuples one after another in the worker that picked it up and answers their
/// results in order. The handle answers one list per part, so the results of the
/// whole fan-out are those lists in order; the last part holds what is left over.
///
/// Batching trades parallelism for dispatch: one message per `size` runs is
/// fewer broker round trips and fewer reservations.
///
/// A slice is one run of the chunk task, so it is retried as a whole by that
/// task's policy — the default one, since the message names the chunk task and
/// not the task inside it — and a tuple that fails stops its slice. A run that
/// needs its own retry budget is sent with `map` or `starmap` instead, where each
/// run carries the policy of the task it belongs to.
pub fn[A, R, B : ToJson] Task::chunks(
  self : Task[A, R],
  tuples : Array[B],
  size~ : Int32 = 1,
) -> Sig raise @spec.Refused {
  if size < 1 {
    raise @spec.Refused::Malformed(got="a chunk of \{size} items")
  }
  let sigs = []
  let mut from = 0
  while from < tuples.length() {
    let upto = if from + size > tuples.length() {
      tuples.length()
    } else {
      from + size
    }
    let items = []
    let mut at = from
    while at < upto {
      items.push(Json(tuples[at]))
      at = at + 1
    }
    let args = Json::object({
      "task": Json::string(self.name),
      "items": Json::array(items),
    })
    sigs.push(
      Sig::of([
        {
          ..@spec.Message::new(self.app.next_id(chunk_name), chunk_name, args),
          queue: self.policy.queue,
        },
      ]),
    )
    from = upto
  }
  group(sigs)
}

///|
/// The task and the tuples a chunk message carries.
fn chunk_spec(spec : Json) -> (String, Array[Json]) raise @spec.Refused {
  let fields = match spec {
    Object(fields) => fields
    _ => raise @spec.Refused::Malformed(got="a chunk that is not an object")
  }
  let name = match fields.get("task") {
    Some(String(name)) => name
    _ => raise @spec.Refused::Malformed(got="a chunk with no task name")
  }
  let items = match fields.get("items") {
    Some(Array(items)) => items
    _ => raise @spec.Refused::Malformed(got="a chunk with no list of items")
  }
  (name, items)
}

///|
/// Run a chunk: its task over each of its tuples, in order, answering the
/// results.
///
/// A tuple that fails stops the slice — the rest of it would have been work done
/// for a result nobody keeps — and the failure is the chunk's, with the words
/// the item failed with rather than a second layer of them.
async fn run_chunk(
  app : App,
  spec : Json,
  context : Context,
) -> Result[Json, String] {
  let (name, items) = chunk_spec(spec) catch {
    error => return Err(error.to_string())
  }
  let entry = match app.tasks.get(name) {
    Some(entry) => entry
    None => return Err("no task registered as " + name)
  }
  let values = []
  for item in items {
    let (args, _) = tuple_value(item) catch {
      error => return Err(error.to_string())
    }
    match (entry.run)(args, { ..context, name, }) {
      Ok(value) => values.push(value)
      Err(reason) => return Err(reason)
    }
  }
  Ok(Json::array(values))
}

///|
/// Register the chunk body. Every worker has it: a worker that never built a
/// canvas still has to run the chunks another process built.
///
/// It goes in as the registry holds a task rather than through `App::task`, so a
/// failed item is recorded with the words it failed with instead of being read
/// back out of a raised error first.
fn App::install_chunks(self : App) -> Unit {
  if self.tasks.contains(chunk_name) {
    return
  }
  self.tasks[chunk_name] = {
    policy,
    run: async fn(spec, context) { run_chunk(self, spec, context) },
  }
}

///|
/// Publish every message of the sig on `app` and return a handle on what it
/// waits for.
///
/// A chord's counter is opened here, before any part can finish: a part that
/// arrived first would otherwise find no counter and take its own arrival for
/// the end of the chord.
///
/// A group with no part in it publishes nothing and hands back a handle that is
/// already answered: a set of nothing is a set that has all settled.
pub async fn Sig::send(
  self : Sig,
  app : App,
) -> Handle[Json] raise @spec.Refused {
  if self.publish.length() == 0 && self.shape == One {
    raise @spec.Refused::Empty(kind="canvas")
  }
  let opened = Map([])
  for message in self.publish {
    match message.chord {
      Some(chord) =>
        if !opened.contains(chord.key) {
          let counter = match app.counter {
            Some(counter) => counter
            None => raise @spec.Refused::Missing(kind="counter")
          }
          ignore(counter.add(chord.key, chord.total))
          opened[chord.key] = ()
        }
      None => ()
    }
  }
  for message in self.publish {
    app.broker.publish(message.queue, message)
  }
  Handle::of_json(app, self.waits, self.shape == All)
}
