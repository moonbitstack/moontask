// The in-memory broker, store, counter and inspector.
//
// Everything a single process needs to run tasks without a server: queues in
// memory, delayed messages promoted when their `eta` comes, results in a map.
// It is what the tests and the examples run on, and it is the shape a broker for
// a real transport is written against.

///|
/// A broker, result store, chord counter and inspector over maps.
///
/// `clock` is the same function the app is given: the broker needs time to know
/// when a delayed message is due, and the library never reads the system clock
/// itself.
pub struct MemoryBroker {
  clock : () -> Int64
  lock : @async.Mutex
  queues : Map[String, Array[@spec.Message]]
  mut delayed : Array[@spec.Message]
  unacked : Map[String, @spec.Delivery]
  results : Map[String, @spec.Outcome]
  counters : Map[String, Int32]
  dead : Array[@spec.Message]
  mut seq : Int32
}

///|
/// An empty broker whose time comes from `clock`.
pub fn MemoryBroker::new(clock~ : () -> Int64) -> MemoryBroker {
  {
    clock,
    lock: @async.Mutex::Mutex(),
    queues: Map([]),
    delayed: [],
    unacked: Map([]),
    results: Map([]),
    counters: Map([]),
    dead: [],
    seq: 0,
  }
}

///|
/// Add a message to the back of its queue.
fn MemoryBroker::enqueue(self : MemoryBroker, message : @spec.Message) -> Unit {
  match self.queues.get(message.queue) {
    Some(list) => list.push(message)
    None => self.queues[message.queue] = [message]
  }
}

///|
/// Move every delayed message whose time has come to its queue.
fn MemoryBroker::promote(self : MemoryBroker) -> Unit {
  if self.delayed.length() == 0 {
    return
  }
  let now = (self.clock)()
  let waiting = []
  for message in self.delayed {
    if message.is_due(now) {
      self.enqueue(message)
    } else {
      waiting.push(message)
    }
  }
  self.delayed = waiting
}

///|
/// Take the next message from `queue`: the highest priority, oldest first within
/// the same priority.
fn MemoryBroker::take(self : MemoryBroker, queue : String) -> @spec.Message? {
  let list = match self.queues.get(queue) {
    Some(list) => list
    None => return None
  }
  if list.length() == 0 {
    return None
  }
  let mut best = 0
  let mut at = 1
  while at < list.length() {
    if list[at].priority > list[best].priority {
      best = at
    }
    at = at + 1
  }
  let message = list[best]
  let rest = []
  let mut index = 0
  for item in list {
    if index != best {
      rest.push(item)
    }
    index = index + 1
  }
  self.queues[queue] = rest
  Some(message)
}

///|
pub impl @spec.Broker for MemoryBroker with fn publish(
  self : MemoryBroker,
  queue : String,
  message : @spec.Message,
) -> Unit raise @spec.Refused {
  self.lock.acquire()
  let now = (self.clock)()
  let routed = { ..message, queue, }
  if routed.is_due(now) {
    self.enqueue(routed)
  } else {
    self.delayed.push(routed)
  }
  self.lock.release()
}

///|
pub impl @spec.Broker for MemoryBroker with fn consume(
  self : MemoryBroker,
  queues : ArrayView[String],
  prefetch~ : Int32,
) -> @spec.Delivery raise @spec.Refused {
  ignore(prefetch)
  while true {
    self.lock.acquire()
    self.promote()
    let mut picked = None
    for queue in queues {
      match self.take(queue) {
        Some(message) =>
          match picked {
            None => picked = Some((queue, message))
            Some((_, held)) =>
              if message.priority > held.priority {
                picked = Some((queue, message))
              }
          }
        None => ()
      }
    }
    match picked {
      Some((queue, message)) => {
        self.seq = self.seq + 1
        let delivery : @spec.Delivery = {
          message,
          queue,
          receipt: self.seq.to_string(),
        }
        self.unacked[delivery.receipt] = delivery
        self.lock.release()
        return delivery
      }
      None => {
        self.lock.release()
        nap(1)
      }
    }
  } nobreak {
    raise @spec.Refused::Missing(kind="broker")
  }
}

///|
pub impl @spec.Broker for MemoryBroker with fn ack(
  self : MemoryBroker,
  delivery : @spec.Delivery,
) -> Unit raise @spec.Refused {
  self.lock.acquire()
  self.unacked.remove(delivery.receipt)
  self.lock.release()
}

///|
pub impl @spec.Broker for MemoryBroker with fn reject(
  self : MemoryBroker,
  delivery : @spec.Delivery,
  requeue~ : Bool,
) -> Unit raise @spec.Refused {
  self.lock.acquire()
  self.unacked.remove(delivery.receipt)
  if requeue {
    self.enqueue(delivery.message)
  } else {
    self.dead.push(delivery.message)
  }
  self.lock.release()
}

///|
pub impl @spec.Backend for MemoryBroker with fn store(
  self : MemoryBroker,
  id : String,
  outcome : @spec.Outcome,
) -> Unit raise @spec.Refused {
  self.lock.acquire()
  self.results[id] = outcome
  self.lock.release()
}

///|
pub impl @spec.Backend for MemoryBroker with fn fetch(
  self : MemoryBroker,
  id : String,
) -> @spec.Outcome? raise @spec.Refused {
  self.lock.acquire()
  let outcome = self.results.get(id)
  self.lock.release()
  outcome
}

///|
pub impl @spec.Counter for MemoryBroker with fn add(
  self : MemoryBroker,
  key : String,
  delta : Int32,
) -> Int32 raise @spec.Refused {
  self.lock.acquire()
  let next = self.counters.get(key).unwrap_or(0) + delta
  self.counters[key] = next
  self.lock.release()
  next
}

///|
pub impl @spec.Counter for MemoryBroker with fn drop(
  self : MemoryBroker,
  key : String,
) -> Unit raise @spec.Refused {
  self.lock.acquire()
  self.counters.remove(key)
  self.lock.release()
}

///|
pub impl @spec.Inspector for MemoryBroker with fn queues(self : MemoryBroker) -> Array[
  @spec.QueueInfo,
] raise @spec.Refused {
  self.lock.acquire()
  self.promote()
  let names = Map([])
  for name, _ in self.queues {
    names[name] = ()
  }
  for message in self.delayed {
    names[message.queue] = ()
  }
  for _, delivery in self.unacked {
    names[delivery.queue] = ()
  }
  let info = []
  for name, _ in names {
    let list = self.queues.get(name).unwrap_or([])
    let mut delayed = 0L
    for message in self.delayed {
      if message.queue == name {
        delayed = delayed + 1L
      }
    }
    let mut unacked = 0L
    for _, delivery in self.unacked {
      if delivery.queue == name {
        unacked = unacked + 1L
      }
    }
    info.push(@spec.QueueInfo::{
      name,
      ready: list.length().to_int64(),
      delayed,
      unacked,
    })
  }
  self.lock.release()
  info
}

///|
pub impl @spec.Inspector for MemoryBroker with fn peek(
  self : MemoryBroker,
  queue : String,
  limit~ : Int32,
) -> Array[@spec.Message] raise @spec.Refused {
  self.lock.acquire()
  self.promote()
  let seen = []
  match self.queues.get(queue) {
    Some(list) => {
      let mut at = 0
      while at < list.length() && at < limit {
        seen.push(list[at])
        at = at + 1
      }
    }
    None => ()
  }
  self.lock.release()
  seen
}

///|
pub extend MemoryBroker with @spec.Broker::{publish, consume, ack, reject}

///|
pub extend MemoryBroker with @spec.Backend::{store, fetch}

///|
pub extend MemoryBroker with @spec.Counter::{add, drop}

///|
pub extend MemoryBroker with @spec.Inspector::{queues, peek}
