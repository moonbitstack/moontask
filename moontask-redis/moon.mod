name = "moonbitstack/moontask-redis"

version = "0.1.0"

readme = "README.md"

repository = "https://github.com/moonbitstack/moontask"

license = "Apache-2.0"

keywords = [
  "task-queue",
  "job-queue",
  "queue",
  "worker",
  "celery",
  "redis",
  "broker",
  "result-backend",
  "resp",
  "distributed",
  "moonbit",
]

description = "Redis broker and result store for moontask: queues in list and sorted-set keys, results and chord counters in string keys."

source = "src"

import {
  "moonbitstack/moontask@0.1.0",
  "moonbitstack/moonredis@0.1.3",
  "moonbitlang/async@0.22.5",
}
