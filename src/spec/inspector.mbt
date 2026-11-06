// What a dashboard asks a broker that a worker never does.

///|
/// A broker that can report its queues and show what waits in them.
///
/// A worker only needs `Broker`, so a broker is not obliged to implement this.
/// A management page is the reason it optionally does: without it the page can
/// show results but not what is still queued.
pub(open) trait Inspector {
  /// Every queue the broker knows, with what waits in each.
  async fn queues(Self) -> Array[QueueInfo] raise Refused
  /// Up to `limit` messages that would be taken next from `queue`, in order,
  /// without claiming them.
  async fn peek(Self, queue : String, limit~ : Int32) -> Array[Message] raise Refused
}
