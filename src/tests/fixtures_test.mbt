// What every test in this package builds on: a clock the test moves by hand, so
// a wait of a minute costs nothing and an `eta` an hour away can be reached in
// one step.

///|
/// A clock a test sets.
priv struct Manual {
  mut at : Int64
}

///|
fn Manual::new(at : Int64) -> Manual {
  { at, }
}

///|
fn Manual::now(self : Manual) -> Int64 {
  self.at
}

///|
fn Manual::advance(self : Manual, by : Int64) -> Unit {
  self.at = self.at + by
}

///|
/// An app over the in-memory broker, which is also its store and its counter,
/// with `clock` driving everything.
fn app_on(clock : () -> Int64) -> (@moontask.App, @moontask.MemoryBroker) {
  let broker = @moontask.MemoryBroker::new(clock~)
  let app = @moontask.App::new(broker, backend=broker, counter=broker, clock~)
  (app, broker)
}

///|
/// Give the workers `ticks` milliseconds of real time to consume, run and
/// settle. A tick is a millisecond because a consumer that finds the queue empty
/// waits that long before asking again, so a shorter settle would inspect a
/// worker that has not had its turn yet.
async fn settle(ticks : Int32) -> Unit {
  let mut left = ticks
  while left > 0 {
    @async.sleep(1)
    left = left - 1
  }
}

///|
/// Block until cancelled. A body that must still be running when the test looks
/// at it uses this instead of a long wait, so cancelling it is what ends it: a
/// wait on the timer is cancelled, a wait on a condition variable is not.
async fn hang() -> Unit {
  @async.sleep(3600000)
}

///|
/// Start `worker` in its own task and stop it when `body` finishes.
async fn[X] with_worker(worker : @moontask.Worker, body : async () -> X) -> X {
  @async.with_task_group(async fn(group) {
    group.spawn_bg(async fn() { worker.run() }, no_wait=true)
    let value = body()
    worker.stop()
    value
  })
}
