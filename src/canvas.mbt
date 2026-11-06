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
/// Publish every message of the sig on `app` and return a handle on what it
/// waits for.
///
/// A chord's counter is opened here, before any part can finish: a part that
/// arrived first would otherwise find no counter and take its own arrival for
/// the end of the chord.
pub async fn Sig::send(
  self : Sig,
  app : App,
) -> Handle[Json] raise @spec.Refused {
  if self.publish.length() == 0 {
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
