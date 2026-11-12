# moontask-sqlite-live

A check of `moontask-sql` against a real SQLite database rather than a driver
that answers from a script: the statements it writes have to be the ones SQLite
accepts, its upsert has to be the one it supports, and a flow running on a worker
has to leave a row behind.

The database is in memory, so there is nothing to start and nothing to clean up,
and the tests always run:

```sh
moon test --target native -p moonbitstack/moontask-sqlite-live/live
```

This module is not published and is not a dependency of the library. It is
listed in the repository's `moon.work` and in `.moonignore`, which keeps it out
of the published tarball.
