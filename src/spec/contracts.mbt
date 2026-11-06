// The three seams. A third party implements these and the library runs on its
// transport, store and counter without a line of this repository changing.

///|
/// Where messages wait and what settles them.
///
/// `consume` blocks until a message is available or the call is cancelled, and
/// hands back a `Delivery` whose `receipt` the same broker settles later. The
/// four calls are all a worker needs: `publish` to enqueue, `consume` to claim,
/// `ack` when the run settled and the message must not come back, `reject` when
/// it must — with `requeue` false it goes to whatever dead letter the broker
/// keeps.
///
/// `prefetch` bounds how many messages one worker holds unsettled at a time, so
/// a fast producer cannot bury a slow one.
pub(open) trait Broker {
  async fn publish(Self, queue : String, message : Message) -> Unit raise Refused
  async fn consume(Self, queues : ArrayView[String], prefetch~ : Int32) -> Delivery raise Refused
  async fn ack(Self, delivery : Delivery) -> Unit raise Refused
  async fn reject(Self, delivery : Delivery, requeue~ : Bool) -> Unit raise Refused
}

///|
/// Where results wait for a caller.
///
/// `fetch` returns `None` both for a run nobody has recorded yet and for a run
/// whose store keeps nothing, which is why a caller that needs the difference
/// asks for a store before it starts.
pub(open) trait Backend {
  async fn store(Self, id : String, outcome : Outcome) -> Unit raise Refused
  async fn fetch(Self, id : String) -> Outcome? raise Refused
}

///|
/// The counter a chord waits on.
///
/// It is separate from `Backend` because counting parts and keeping results are
/// different guarantees: a store may keep results for an hour and a counter for
/// a minute, and a chord only needs the counter. `add` returns the value after
/// the add, so the part that brings the count to zero is the one that fires the
/// callback; it must be atomic, since two parts finishing at once would
/// otherwise both see a non-zero count or both see zero.
pub(open) trait Counter {
  async fn add(Self, key : String, delta : Int32) -> Int32 raise Refused
  async fn drop(Self, key : String) -> Unit raise Refused
}
