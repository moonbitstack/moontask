// The scheduler: the loop that fires the jobs of one process.

///|
/// A set of jobs and the clock they run against.
///
/// The scheduler holds no queue and no store: a job that must survive a restart,
/// run on another machine, or be retried belongs on a task instead. What it does
/// keep is the rhythm — the next run of each job is computed from the run that
/// was due, not from the moment the loop happened to wake, so a loop that was
/// busy for a while does not drag every job with it.
pub struct Scheduler {
  clock : () -> Int64
  jobs : Array[Job]
  mut stopping : Bool
}

///|
/// A scheduler whose only source of time is `clock`.
pub fn Scheduler::new(clock~ : () -> Int64) -> Scheduler {
  { clock, jobs: [], stopping: false, }
}

///|
/// Take `job` into the schedule, with its first run computed from now.
///
/// The job is held as it was given: changing it afterwards changes nothing, so
/// build the chain before adding it.
pub fn Scheduler::add(self : Scheduler, job : Job) -> Unit {
  let now = (self.clock)()
  let scheduled = { ..job, next: job.next_after(now), }
  self.jobs.push(scheduled)
}

///|
/// Every job, soonest first, with how many times each has run.
pub fn Scheduler::pending(self : Scheduler) -> Array[Job] {
  let jobs = []
  for job in self.jobs {
    jobs.push(job)
  }
  jobs.sort_by(fn(left, right) { left.next_at().compare(right.next_at()) })
  jobs
}

///|
/// Stop after the run in progress. The loop wakes at least every 50 milliseconds,
/// so this takes effect promptly even with nothing scheduled.
pub fn Scheduler::stop(self : Scheduler) -> Unit {
  self.stopping = true
}

///|
/// Fire every job whose time has come, then wait for the next one, until `stop`.
///
/// A job that raises is counted and the loop goes on: one failing chore must not
/// stop the others, and a process that dies on it loses every rhythm it had.
pub async fn Scheduler::run(self : Scheduler) -> Unit {
  while !self.stopping {
    let now = (self.clock)()
    for job in self.jobs {
      while job.next <= now {
        match job.action {
          Some(action) =>
            action() catch {
              _ => job.failures = job.failures + 1
            }
          None => ()
        }
        job.runs = job.runs + 1
        job.next = job.next_after(job.next)
      }
    }
    let mut wait = 50L
    let now = (self.clock)()
    for job in self.jobs {
      let left = job.next - now
      if left < wait {
        wait = left
      }
    }
    @wait.nap(if wait > 0L { wait.to_int() } else { 1 })
  }
}
