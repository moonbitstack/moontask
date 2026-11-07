// A superstep of a state graph, run where the workers are.

///|
/// The task behind a node, and the nodes whose answers it reads.
struct Wiring {
  task : String
  deps : Array[String]
}

///|
/// The executor a state graph runs on: every work item of a superstep becomes a
/// queue message, and what comes back is the write that step contributes.
///
/// A graph keeps the order, the durability and the retry policy of a run; this
/// only decides where a node's body runs. Here it runs on the workers, so a
/// workflow outlives the process that started it.
///
/// Which task a node runs, and what it is called with, is the app's route for
/// its graph and node, which is what a flow registers: a node that waits for
/// nothing is called with the run's input, and a node that waits for others with
/// an object of their answers under their ids. A node with no route writes
/// nothing, and a graph with several roots uses one such node as its entry.
pub struct Dispatch {
  app : App
  queue : String
}

///|
/// The route a graph node goes by.
fn App::route_of(self : App, graph : String, node : String) -> Wiring? {
  self.routes.get(graph + "/" + node)
}

///|
/// Record that `node` of `graph` runs the task `task`, and reads `deps`.
///
/// A flow registers this for every step it holds, which is how an executor — in
/// this process or in another — finds the body behind a node name.
pub fn App::route(
  self : App,
  graph : String,
  node : String,
  task : String,
  deps~ : Array[String] = [],
) -> Unit {
  self.routes[graph + "/" + node] = { task, deps: deps.copy(), }
}

///|
/// An executor that sends the nodes of a superstep to `queue` and waits for each
/// result.
pub fn App::executor(self : App, queue~ : String = "default") -> Dispatch {
  { app: self, queue, }
}

///|
/// The answers `deps` name, out of the run's state.
fn picked(state : Json, deps : Array[String]) -> Json {
  let out : Map[String, Json] = Map([])
  match state {
    Object(fields) =>
      for dep in deps {
        out[dep] = fields.get(dep).unwrap_or(Json::null())
      }
    // A run whose input is not an object has nothing to hand to a step that
    // waits for others; an empty object says so without inventing a value.
    _ => ()
  }
  Json(out)
}

///|
/// What one node answers: the write that names it, so the graph's merge function
/// can file the value under the step it came from.
fn written(step : String, output : Json) -> Json {
  { "kind": "write", "value": { "step": step, "output": output } }
}

///|
/// Send one node's work and wait for what it wrote. The wait is the worker's
/// run: a step that fails for good comes back as an error string, which the
/// engine turns into a failed node.
async fn Dispatch::one(
  self : Dispatch,
  work : @moonstate.Work,
) -> @moonstate.Done {
  let route = match self.app.route_of(work.graph, work.node) {
    Some(route) => route
    // A node nobody routed writes nothing: the entry of a graph with several
    // roots is one, and so is a node that only exists to join edges.
    None => return { values: Some([]), error: None, }
  }
  let state = (work.input)()
  let args = if route.deps.is_empty() {
    state
  } else {
    picked(state, route.deps)
  }
  // A refusal keeps its own words here rather than being flattened into the
  // name of the variant: what a task failed with is what the run records.
  let sent = Ok(self.app.send(route.task, args, queue=self.queue)) catch {
    refused => Err(refused)
  }
  match sent {
    Err(refused) => { values: None, error: Some(refused.to_string()), }
    Ok(handle) => {
      let waited = Ok(handle.get()) catch { refused => Err(refused) }
      match waited {
        Err(refused) => { values: None, error: Some(refused.to_string()), }
        Ok(output) =>
          { values: Some([written(work.node, output)]), error: None, }
      }
    }
  }
}

///|
pub extend Dispatch with @moonstate.Executor::{each}

///|
/// The superstep, dispatched. Every node of a batch is sent at once, which is
/// what makes a fan-out parallel, and the answers come back in the order the
/// work was given.
pub impl @moonstate.Executor for Dispatch with fn each(self, works) -> Array[
  @moonstate.Done,
] raise {
  let jobs : Array[async () -> @moonstate.Done] = []
  for work in works {
    jobs.push(async fn() { self.one(work) })
  }
  @async.all(jobs[:])
}
