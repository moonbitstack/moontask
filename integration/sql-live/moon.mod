name = "moonbitstack/moontask-sql-live"

version = "0.1.0"

readme = "README.md"

repository = "https://github.com/moonbitstack/moontask"

license = "Apache-2.0"

description = "A live check that the SQL store and the run records work against a real PostgreSQL, with a real worker running a real flow."

source = "src"

import {
  "moonbitstack/moontask@0.1.0",
  "moonbitstack/moontask-sql@0.1.0",
  "moonbitstack/moondb@0.2.7",
  "moonbitstack/moonpostgres@0.6.10",
  "moonbitlang/async@0.22.5",
}
