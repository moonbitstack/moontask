name = "moonbitstack/moontask-sql"

version = "0.1.0"

readme = "README.md"

repository = "https://github.com/moonbitstack/moontask"

license = "Apache-2.0"

keywords = [
  "task-queue",
  "queue",
  "worker",
  "result-backend",
  "sql",
  "database",
  "postgres",
  "mysql",
  "sqlite",
  "workflow",
  "run-records",
  "moonbit",
]

description = "SQL result store and workflow run records for moontask, over any moondb AsyncDriver the caller brings."

source = "src"

import {
  "moonbitstack/moontask@0.1.0",
  "moonbitstack/moondb@0.2.7",
  "moonbitlang/async@0.22.5",
}
