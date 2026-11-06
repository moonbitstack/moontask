name = "moonbitstack/moontask-admin"

version = "0.1.0"

readme = "README.md"

repository = "https://github.com/moonbitstack/moontask"

license = "Apache-2.0"

keywords = [
  "task-queue",
  "job-queue",
  "queue",
  "admin",
  "dashboard",
  "management",
  "monitoring",
  "asgi",
  "sse",
  "server-sent-events",
  "moonbit",
]

description = "A management interface for moontask: an ASGI application serving queues, tasks and results, with a page and a live event stream."

source = "src"

import {
  "moonbitstack/moontask@0.1.0",
  "moonbitstack/moonasgi@0.10.6",
  "moonbitlang/async@0.22.5",
}
