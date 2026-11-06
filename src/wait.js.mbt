// Waiting on a target with a timer.
//
// Every wait in this library goes through `nap`, so the queue, the store and the
// handle never name the event loop's timer themselves and their signatures stay
// to the one error this library raises.

///|
/// Wait `milliseconds`. Never refuses: a task queue that cannot wait would
/// rather be slow than fail a run.
pub async fn nap(milliseconds : Int32) -> Unit noraise {
  @async.sleep(milliseconds)
}
