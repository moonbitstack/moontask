name = "moonbitstack/moontask"

version = "0.1.1"

readme = "README.md"

repository = "https://github.com/moonbitstack/moontask"

license = "Apache-2.0"

keywords = [
  "task-queue",
  "job-queue",
  "queue",
  "worker",
  "celery",
  "scheduler",
  "cron",
  "crontab",
  "schedule",
  "retry",
  "backoff",
  "prefetch",
  "workflow",
  "canvas",
  "chain",
  "group",
  "chord",
  "dag",
  "beat",
  "periodic",
  "background-jobs",
  "distributed",
  "moonbit",
]

description = "Task queues and schedules for MoonBit: workers, retries, periodic beats, and the chain, group and chord canvas. Brokers and result stores are separate modules."

source = "src"

import {
  "moonbitstack/moondate@0.1.7",
  "moonbitstack/moonjson@0.4.4",
  "moonbitstack/moonbase@0.4.5",
  "moonbitlang/async@0.22.5",
  "moonbitstack/moonstate@0.1.1",
  "moonbitstack/moonast@0.2.8",
  "moonbitlang/parser@0.4.8",
}
