# moontask-admin

A management interface for [moontask](../README.md): what is queued, what is registered, how each run is doing, and a live stream of counts.

```moonbit
let admin = @admin.Admin::new(app, inspector=broker)
let asgi = admin.to_asgi()
```

`asgi` is an ASGI application: a server runs it, a framework mounts it, and a test drives it without a socket. The module is separate from the queue so that a process which only works does not download a web stack.
