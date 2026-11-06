// A task: a body with a name and a policy, and the message it turns into.

///|
/// A registered body. Sending it writes a message and hands back a handle.
///
/// The argument type is what the body takes; the result type is what it returns,
/// and the handle decodes the stored value back into it. Both go over the wire as
/// JSON, so a task may be sent by one process and run by another.
pub struct Task[A, R] {
  app : App
  name : String
  policy : Policy
  run : async (A, Context) -> R
}

///|
/// The name the message carries.
pub fn[A, R] Task::name(self : Task[A, R]) -> String {
  self.name
}

///|
/// The policy the registration was given.
pub fn[A, R] Task::policy(self : Task[A, R]) -> Policy {
  self.policy
}

///|
/// The message `args` becomes, without publishing it.
fn[A : ToJson, R] Task::compose(
  self : Task[A, R],
  args : A,
  eta? : Int64,
  countdown? : Int64,
  expires? : Int64,
  queue? : String,
  priority? : Int32,
  id? : String,
) -> @spec.Message {
  let now = self.app.now()
  let eta = match (eta, countdown) {
    (Some(at), _) => Some(at)
    (None, Some(delay)) => Some(now + delay)
    (None, None) => None
  }
  {
    id: id.unwrap_or(self.app.next_id(self.name)),
    name: self.name,
    args: Json(args),
    queue: queue.unwrap_or(self.policy.queue),
    priority: priority.unwrap_or(0),
    eta,
    expires,
    retries: 0,
    chain: [],
    chord: None,
    single_arg: false,
    immutable: false,
  }
}

///|
/// Publish `args` and return a handle on the run.
///
/// `eta` is when the message may first run and `countdown` is how long from now
/// that is; `eta` wins when both are given, since it is the more specific of the
/// two. `expires` is the time after which running it is pointless: a worker that
/// finds it expired revokes it instead of running it late.
pub async fn[A : ToJson, R : FromJson] Task::send(
  self : Task[A, R],
  args : A,
  eta? : Int64,
  countdown? : Int64,
  expires? : Int64,
  queue? : String,
  priority? : Int32,
  id? : String,
) -> Handle[R] raise @spec.Refused {
  let message = self.compose(
    args,
    eta?,
    countdown?,
    expires?,
    queue?,
    priority?,
    id?,
  )
  self.app.broker.publish(message.queue, message)
  Handle::of(self.app, [message.id], false)
}

///|
/// The run of `args` as a node of a canvas, without publishing it.
pub fn[A : ToJson, R] Task::sig(self : Task[A, R], args : A) -> Sig {
  Sig::of([self.compose(args)])
}
