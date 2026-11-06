// Waiting on a target without a timer.
//
// The event loop has no clock under WebAssembly; a wait has to come from the
// host. Handing control back to the loop once is the closest this target gets,
// and it is enough for the queue to make progress.

///|
/// Hand the loop back once. The duration is ignored on this target.
pub async fn nap(milliseconds : Int32) -> Unit noraise {
  ignore(milliseconds)
  @async.pause()
}
