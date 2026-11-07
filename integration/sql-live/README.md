# moontask-sql against a real PostgreSQL

This module is not part of the library. It exists so the SQL store is checked
against a server rather than against a script: a real `postgres:16`, a real
socket, a real worker running a real flow whose steps write rows.

```sh
MOONTASK_SQL_LIVE=1 \
MOONTASK_SQL_ADDR=127.0.0.1:5432 \
MOONTASK_SQL_USER=postgres \
MOONTASK_SQL_PASSWORD=postgres \
MOONTASK_SQL_DATABASE=test \
moon test --target native
```

`MOONTASK_SQL_LIVE` is what CI sets. Without it the tests return early, so a
machine with no PostgreSQL still runs the rest of the suite; with it, a server
that does not answer is a failure, not a skip.
