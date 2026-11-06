// The scheduler loop, driven by a clock the test moves.

///|
async test "a job fires once its period has passed" {
  let manual = Manual::new(1_000_000L)
  let fired = []
  let scheduler = @schedule.Scheduler::new(clock=fn() { manual.now() })
  scheduler.add(@schedule.every(1).seconds().run(fn() { fired.push(1) }))
  @async.with_task_group(async fn(group) {
    group.spawn_bg(async fn() { scheduler.run() }, no_wait=true)
    settle(20)
    assert_eq(fired.length(), 0)
    manual.advance(1000L)
    settle(60)
    assert_eq(fired.length(), 1)
    manual.advance(1000L)
    settle(60)
    assert_eq(fired.length(), 2)
    scheduler.stop()
  })
}

///|
async test "a job that raises is counted and the others keep running" {
  let manual = Manual::new(1_000_000L)
  let healthy = []
  let scheduler = @schedule.Scheduler::new(clock=fn() { manual.now() })
  let broken = @schedule.every(1).seconds().run(fn() raise { fail("broken") })
  let fine = @schedule.every(1).seconds().run(fn() { healthy.push(1) })
  scheduler.add(broken)
  scheduler.add(fine)
  @async.with_task_group(async fn(group) {
    group.spawn_bg(async fn() { scheduler.run() }, no_wait=true)
    settle(20)
    manual.advance(1000L)
    settle(80)
    assert_eq(healthy.length(), 1)
    let pending = scheduler.pending()
    assert_eq(pending.length(), 2)
    assert_eq(pending[0].failures() + pending[1].failures(), 1)
    scheduler.stop()
  })
}

///|
async test "pending lists the rhythm that comes next" {
  let manual = Manual::new(1_000_000L)
  let scheduler = @schedule.Scheduler::new(clock=fn() { manual.now() })
  scheduler.add(@schedule.every(10).minutes().named("slow").run(fn() {  }))
  scheduler.add(@schedule.every(1).seconds().named("fast").run(fn() {  }))
  let pending = scheduler.pending()
  assert_eq(pending[0].name(), "fast")
  assert_eq(pending[1].name(), "slow")
}
