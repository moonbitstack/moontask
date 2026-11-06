// The app: the object a caller registers tasks on, sends them through, and
// starts workers and beats from.

///|
/// What a task body is told about the run it is in.
///
/// `retries` is how many attempts before this one failed, `attempt` is this one
/// counting from one, and `last` says whether another attempt follows a failure
/// — a body that must not repeat a side effect checks it before doing the work
/// again.
#derive(Eq, Debug)
pub(all) struct Context {
  id : String
  name : String
  queue : String
  retries : Int32
  attempt : Int32
  last : Bool
  /// The time the run started, from the app's clock, in milliseconds.
  now : Int64
}

///|
pub extend Context with Eq::{not_equal, equal}

///|
pub extend Context with Debug::{to_repr}

///|
/// A registered task, as the registry holds it: the policy and a body that takes
/// the wire argument and answers in strings, so one map holds tasks of every
/// type.
struct Registered {
  policy : Policy
  run : async (Json, Context) -> Result[Json, String]
}

///|
/// The object every other type hangs off.
///
/// It owns the three seams (broker, result store, chord counter), the clock the
/// library reads instead of the system clock, and the registry that turns a
/// message name back into a body. Nothing here is global: two apps in one
/// process do not see each other's tasks.
pub struct App {
  broker : &@spec.Broker
  backend : &@spec.Backend?
  counter : &@spec.Counter?
  clock : () -> Int64
  tasks : Map[String, Registered]
  /// The tasks a worker is running right now, so `revoke(terminate=true)` can
  /// reach the one that is already going.
  running : Map[String, @async.Task[Result[Json, String]]]
  mut seq : Int32
}

///|
/// An app on `broker`, with `backend` keeping results, `counter` counting chord
/// parts, and `clock` as the only source of time.
///
/// The store and the counter are optional: an app without a store runs tasks
/// nobody waits on, and an app without a counter refuses to build a chord. A
/// broker that also implements `Backend` and `Counter` — the in-memory one and
/// the Redis one both do — is passed three times.
pub fn App::new(
  broker : &@spec.Broker,
  backend? : &@spec.Backend,
  counter? : &@spec.Counter,
  clock~ : () -> Int64,
) -> App {
  { broker, backend, counter, clock, tasks: Map([]), running: Map([]), seq: 0, }
}

///|
/// The app's time, in milliseconds since the epoch.
pub fn App::now(self : App) -> Int64 {
  (self.clock)()
}

///|
/// A name nothing else in this app has, for the message of a run nobody named.
fn App::next_id(self : App, name : String) -> String {
  self.seq = self.seq + 1
  "\{self.now()}:\{self.seq}:\{name}"
}

///|
/// Register `run` under `name` and return the task a caller sends.
///
/// Registering twice under the same name replaces the first, which is what makes
/// it possible to run the same task under two policies in one test.
pub fn[A : FromJson, R : ToJson] App::task(
  self : App,
  name : String,
  run : async (A, Context) -> R,
  policy~ : Policy = policy,
) -> Task[A, R] {
  let registered : Registered = {
    policy,
    run: async fn(args, context) {
      let value : A = @json.from_json(args) catch {
        error => return Err("argument does not decode: " + error.to_string())
      }
      let result = run(value, context) catch {
        error => return Err(error.to_string())
      }
      Ok(Json(result))
    },
  }
  self.tasks[name] = registered
  { app: self, name, policy, run, }
}

///|
/// The policy registered under `name`, or `None` when nobody registered it.
pub fn App::policy_of(self : App, name : String) -> Policy? {
  self.tasks.get(name).map(fn(entry) { entry.policy })
}

///|
/// Whether a body is registered under `name`.
pub fn App::has_task(self : App, name : String) -> Bool {
  self.tasks.contains(name)
}

///|
/// Every registered name, sorted so a page listing them is stable.
pub fn App::task_names(self : App) -> Array[String] {
  let names = []
  for name, _ in self.tasks {
    names.push(name)
  }
  names.sort()
  names
}

///|
/// A worker over `queues` that runs up to `concurrency` tasks at once.
///
/// `prefetch` is how many messages one worker may hold unsettled: a worker that
/// holds more than it can run looks busy while the queue behind it starves.
pub fn App::worker(
  self : App,
  queues~ : Array[String] = ["default"],
  concurrency~ : Int32 = 4,
  prefetch~ : Int32 = 4,
) -> Worker {
  Worker::new(self, queues, concurrency, prefetch)
}
