// The worker: the loop that takes messages off a broker and runs them.

///|
/// How a task is allowed to go, per worker: `tokens` runs left in the window
/// that ends at `reset`.
struct Limiter {
  mut tokens : Int32
  mut reset : Int64
}

///|
/// The object that runs tasks.
///
/// A worker consumes from `queues`, runs up to `concurrency` bodies at once, and
/// settles each message according to the task's policy: a failure within the
/// retry budget goes back on the queue with an `eta` computed from the backoff, a
/// success advances the chain or the chord the message carries.
///
/// `stop` is graceful: it stops taking new messages, waits up to `grace`
/// milliseconds for the ones in flight, then cancels whatever is left.
pub struct Worker {
  app : App
  queues : Array[String]
  concurrency : Int32
  prefetch : Int32
  limiters : Map[String, Limiter]
  consumers : Array[@async.Task[Unit]]
  mut inflight : Int32
  mut stopping : Bool
  done : @async.CondVar
  mut failure : Error?
}

///|
fn Worker::new(
  app : App,
  queues : Array[String],
  concurrency : Int32,
  prefetch : Int32,
) -> Worker {
  {
    app,
    queues,
    concurrency,
    prefetch,
    limiters: Map([]),
    consumers: [],
    inflight: 0,
    stopping: false,
    done: @async.CondVar::Cond(),
    failure: None,
  }
}

///|
/// The queues this worker takes from.
pub fn Worker::queues(self : Worker) -> Array[String] {
  self.queues
}

///|
/// The most messages this worker holds unsettled at once.
pub fn Worker::prefetch(self : Worker) -> Int32 {
  self.prefetch
}

///|
/// Whether `stop` has been called.
pub fn Worker::is_stopping(self : Worker) -> Bool {
  self.stopping
}

///|
/// Run until `stop` is called. The call returns once every consumer has ended,
/// which is why a caller runs it in its own task.
pub async fn Worker::run(self : Worker) -> Unit {
  self.stopping = false
  self.inflight = 0
  @async.with_task_group(async fn(group) {
    let mut started = 0
    while started < self.concurrency {
      let consumer = group.spawn(
        async fn() {
          ignore(
            @async.handle_cancellation(async fn() { self.consume() }) catch {
              error => {
                self.failure = Some(error)
                None
              }
            },
          )
        },
        no_wait=true,
      )
      self.consumers.push(consumer)
      started = started + 1
    }
    self.done.wait()
  })
  self.consumers.clear()
  match self.failure {
    Some(error) => raise error
    None => ()
  }
}

///|
/// Stop taking new work, give what is running up to `grace` milliseconds to
/// finish, then cancel the rest.
pub async fn Worker::stop(self : Worker, grace~ : Int64 = 30000L) -> Unit {
  if self.stopping {
    self.done.broadcast()
    return
  }
  self.stopping = true
  // The grace is real time on purpose: it bounds how long already-started work
  // may take to finish, which is a fact about the process, not about the task
  // clock the app was given.
  let deadline = @async.now() + grace
  while self.inflight > 0 && @async.now() < deadline {
    @wait.nap(2)
  }
  for consumer in self.consumers {
    consumer.cancel()
  }
  self.done.broadcast()
}

///|
/// One consumer: take a message, settle it, repeat.
async fn Worker::consume(self : Worker) -> Unit {
  while !self.stopping {
    let delivery = self.app.broker.consume(self.queues, prefetch=self.prefetch)
    self.inflight = self.inflight + 1
    self.handle(delivery)
    self.inflight = self.inflight - 1
  }
}

///|
/// Settle one delivery: run it, record what happened, settle the message and
/// advance whatever it carries.
async fn Worker::handle(self : Worker, delivery : @spec.Delivery) -> Unit {
  let message = delivery.message
  let app = self.app
  let now = app.now()
  let entry = match app.tasks.get(message.name) {
    Some(entry) => entry
    None => {
      self.record(
        message.id,
        @spec.outcome_failed(
          "no task registered as " + message.name,
          message.retries,
          now,
        ),
        true,
      )
      app.broker.reject(delivery, requeue=false)
      return
    }
  }
  if message.is_expired(now) {
    self.record(
      message.id,
      @spec.Outcome::{
        state: @spec.Revoked,
        result: None,
        error: Some("expired before it was run"),
        retries: message.retries,
        at: now,
      },
      true,
    )
    app.broker.ack(delivery)
    return
  }
  // A revoke that arrived while the message waited in the queue wins over the
  // run: the caller asked for it before anything happened.
  let already_revoked = match app.backend {
    Some(backend) =>
      match backend.fetch(message.id) {
        Some(outcome) => outcome.state == @spec.Revoked
        None => false
      }
    None => false
  }
  if already_revoked {
    app.broker.ack(delivery)
    return
  }
  let policy = entry.policy
  if !policy.acks_late {
    app.broker.ack(delivery)
  }
  self.wait_for_rate(message.name, policy.rate)
  let context : Context = {
    id: message.id,
    name: message.name,
    queue: delivery.queue,
    retries: message.retries,
    attempt: message.retries + 1,
    last: message.retries + 1 > policy.retries,
    now,
  }
  let result = @async.with_task_group(async fn(group) {
    let body = group.spawn(
      async fn() { run_within(entry, message.args, context, policy.timeout) },
      no_wait=true,
    )
    app.running[message.id] = body
    let settled : Result[Json, String]? = @async.handle_cancellation(async fn() {
      body.wait()
    }) catch {
      // `revoke(terminate=true)` cancels the body itself; waiting on a task that
      // was cancelled from outside raises rather than answering.
      _ => None
    }
    app.running.remove(message.id)
    settled
  })
  let at = app.now()
  let keep = !policy.ignore_result
  match result {
    None => {
      let outcome : @spec.Outcome = {
        state: @spec.Revoked,
        result: None,
        error: Some("terminated while running"),
        retries: message.retries,
        at,
      }
      self.record(message.id, outcome, keep)
      self.settle_chord(message, outcome)
    }
    Some(Ok(value)) => {
      let outcome : @spec.Outcome = {
        state: @spec.Succeeded,
        result: Some(value),
        error: None,
        retries: message.retries,
        at,
      }
      self.record(message.id, outcome, keep)
      self.advance_chain(message, value)
      self.settle_chord(message, outcome)
    }
    Some(Err(reason)) =>
      if message.retries < policy.retries {
        let attempt = message.retries + 1
        let retry = {
          ..message,
          retries: attempt,
          eta: Some(at + policy.backoff_delay(attempt + 1, message.id)),
        }
        self.record(
          message.id,
          @spec.Outcome::{
            state: @spec.Retrying,
            result: None,
            error: Some(reason),
            retries: attempt,
            at,
          },
          keep,
        )
        app.broker.publish(retry.queue, retry)
      } else {
        let outcome = @spec.outcome_failed(reason, message.retries, at)
        self.record(message.id, outcome, keep)
        self.settle_chord(message, outcome)
      }
  }
  if policy.acks_late {
    app.broker.ack(delivery)
  }
}

///|
/// Run the body, cut short at `timeout` milliseconds when the policy sets one.
async fn run_within(
  entry : Registered,
  args : Json,
  context : Context,
  timeout : Int64?,
) -> Result[Json, String] {
  match timeout {
    Some(limit) =>
      @async.with_timeout(limit.to_int(), async fn() {
        (entry.run)(args, context)
      }) catch {
        _ => Err("timed out after \{limit} ms")
      }
    None => (entry.run)(args, context)
  }
}

///|
/// Keep the store up to date, unless the task asked for its results to be
/// dropped.
async fn Worker::record(
  self : Worker,
  id : String,
  outcome : @spec.Outcome,
  keep : Bool,
) -> Unit raise @spec.Refused {
  if !keep {
    return
  }
  match self.app.backend {
    Some(backend) => backend.store(id, outcome)
    None => ()
  }
}

///|
/// Send the next link of a chain once this one succeeded.
async fn Worker::advance_chain(
  self : Worker,
  message : @spec.Message,
  value : Json,
) -> Unit raise @spec.Refused {
  if message.chain.length() == 0 {
    return
  }
  let next = message.chain[0]
  let rest = []
  let mut at = 1
  while at < message.chain.length() {
    rest.push(message.chain[at])
    at = at + 1
  }
  // A link that is not immutable is handed this link's result as its first
  // argument, which is what Celery's chains do; the links a canvas here builds
  // are immutable and get exactly the arguments they declared.
  let args = if next.immutable {
    next.args
  } else {
    match next.args {
      Array(declared) => {
        let merged = [value]
        for item in declared {
          merged.push(item)
        }
        Json::array(merged)
      }
      other => Json::array([value, other])
    }
  }
  self.app.broker.publish(next.queue, {
    ..next,
    args,
    single_arg: false,
    chain: rest,
  })
}

///|
/// Count this part against its chord and, when it was the last one, settle the
/// callback: sent with every part's result when they all succeeded, recorded as
/// failed when one did not.
///
/// A part is counted once it will not run again — after it succeeded, failed for
/// good or was revoked — so a part that is only going to be retried leaves the
/// chord open. Where the results are collected is the counter's business: a
/// counter in memory reads them from the store beside it, and Celery's redis
/// counter stashes each one as the part settles, in the same sorted set Celery
/// joins on, so parts run on either side of the boundary complete one chord.
async fn Worker::settle_chord(
  self : Worker,
  message : @spec.Message,
  outcome : @spec.Outcome,
) -> Unit raise @spec.Refused {
  let chord = match message.chord {
    Some(chord) => chord
    None => return
  }
  let counter = match self.app.counter {
    Some(counter) => counter
    None => raise @spec.Refused::Missing(kind="counter")
  }
  match counter.settle(chord, message.id, outcome) {
    @spec.ChordState::Waiting(_) => ()
    @spec.ChordState::Settled(values~, failure~) =>
      match failure {
        Some(reason) => {
          let backend = match self.app.backend {
            Some(backend) => backend
            None => raise @spec.Refused::Missing(kind="backend")
          }
          backend.store(
            chord.callback.id,
            @spec.outcome_failed("chord part failed: " + reason, 0, outcome.at),
          )
        }
        None => {
          // One argument, the list of results: a callback is called with the
          // list, not with the list spread into its arguments.
          let callback = {
            ..chord.callback,
            args: Json::array(values),
            single_arg: true,
          }
          self.app.broker.publish(callback.queue, callback)
        }
      }
  }
}

///|
/// Hold a task to its rate, waiting in this worker until a token is free.
///
/// The window is per worker and per task name: a rate is a promise about how
/// fast one worker asks an external service, not about the whole cluster, which
/// the broker would have to enforce.
async fn Worker::wait_for_rate(
  self : Worker,
  name : String,
  rate : Rate?,
) -> Unit {
  let rate = match rate {
    Some(rate) => rate
    None => return
  }
  while true {
    let limiter = match self.limiters.get(name) {
      Some(limiter) => limiter
      None => {
        let limiter : Limiter = {
          tokens: rate.count,
          reset: self.app.now() + rate.per,
        }
        self.limiters[name] = limiter
        limiter
      }
    }
    let now = self.app.now()
    if now >= limiter.reset {
      limiter.tokens = rate.count
      limiter.reset = now + rate.per
    }
    if limiter.tokens > 0 {
      limiter.tokens = limiter.tokens - 1
      return
    }
    // Wait in short steps and look at the clock again each time: the clock is
    // the caller's, so a window that turns over sooner than the first estimate
    // must not hold the run back.
    let wait = limiter.reset - now
    @wait.nap(if wait > 5L { 5 } else if wait > 0L { wait.to_int() } else { 1 })
  }
}
