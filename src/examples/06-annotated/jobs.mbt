// Copyright 2026 Leo Cheng
// SPDX-License-Identifier: Apache-2.0

///|
/// An order as its message carries it.
#derive(ToJson, FromJson)
pub(all) struct Order {
  id : String
  cups : Int32
}

///|
/// A large order fails its first attempt, so the retry the annotation allows is seen to run.
#task.def(name="orders.place", queue="orders", retries="1")
pub fn place(order : Order, ctx : @moontask.Context) -> String raise {
  if ctx.attempt == 1 && order.cups > 9 {
    fail("too many cups for one try")
  }
  "\{order.id}: \{order.cups} cups on attempt \{ctx.attempt}"
}

///|
/// A plain function with no argument and no use for the run's context is a task as it stands.
#task.def
pub fn tick() -> Unit {
  ()
}
