# moontask-redis

A broker and a result store for [moontask](../README.md) on Redis: queues in lists, delayed messages in sorted sets, and results in string keys.

```moonbit
let conn = @socket.Connection::connect(@moonredis.ClientOptions::at("127.0.0.1", 6379))
let clock = fn() { @env.now() }
let broker = @redis.Broker::new(conn, clock~)
let app = @moontask.App::new(broker, backend=broker.store(), counter=broker.store(), clock~)
```

The module is separate from `moontask` so that a queue on the in-memory broker does not download a Redis client.
