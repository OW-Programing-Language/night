# Redis Streams connector for OW (experimental)

`NightRedisStreams` is an OW-native, synchronous **at-least-once** producer / consumer-group adapter for Redis 6.2+. It shares the configured, TLS-validated Redis client with `NightRedis`, but its keys use `<NIGHT_REDIS_NAMESPACE>:streams/<stream>` and cannot collide with cache keys.

## Configuration

Use the same settings as the [Redis cache starter](redis.md): `NIGHT_REDIS_URL=rediss://...` and optionally `NIGHT_REDIS_NAMESPACE=night`. Use a scoped Redis ACL user, server certificate verification and a persistent Redis deployment (AOF/replication/backup as needed). The `redis://` exception requires both a **loopback** hostname and `NIGHT_REDIS_ALLOW_INSECURE_LOOPBACK=true`, intended solely for disposable integration testing.

Grant the minimum Redis ACL permissions for `XADD`, `XGROUP`, `XREADGROUP`, `XACK`, `XAUTOCLAIM` on just your stream key prefix. No user-supplied URL, Redis command, or arbitrary callback is evaluated by OW.

## OW API

```ow
import "../../src/modules/messaging/NightRedisStreams.ow"

set bus: new NightRedisStreams()
call: bus.open()
call: bus.create_group("orders", "processors")

# Publisher: returns a Redis stream ID (such as 1716998413541-0).
set published_id: bus.publish("orders", "order-created-1001", "{\"orderId\":1001}")

# Poll a consumer group. The count must be 1..32 and the call does not block.
set batch: bus.poll("orders", "processors", "workerA", 8)
print: batch
# batch = {present: 1, events: [{stream_id, message_id, body}, ...]}.
# On each successful idempotent business effect, ACK the returned stream_id.
```

All operations are exposed via the OW `RuntimeExtension` SPI and require the platform JAR in the runtime classpath. Operations:

| Method | Result | Notes |
|---|---|---|
| `open()` | 1 | Shares the Redis TLS / ACL client with the Redis cache |
| `create_group(stream, group)` | 1 | Idempotent: starts at `0-0`, never resets an existing group's cursor |
| `publish(stream, message_id, json_body)` | Redis stream entry ID | Max JSON payload 256 KiB UTF-8; accepts application JSON as a string |
| `poll(stream, group, consumer, count)` | `{present,events}` | Nonblocking, fetches only new group entries |
| `acknowledge(stream, group, stream_id)` | 1 or 0 | 1 means a pending entry was acknowledged |
| `reclaim(stream, group, consumer, min_idle_ms, cursor, count)` | `{present,events,next_cursor}` | Recovers idle, unacknowledged entries |

Names (`stream`, `group`, `consumer`) accept 1–64 safe ASCII letters/digits/underscores/dashes and must start with a letter. Message IDs are restricted to safe ASCII, 8–80 characters. Poll/reclaim counts are limited to 1–32, and `min_idle_ms` to 1,000–86,400,000. No arbitrary Redis key names can be supplied.

## Recovery and delivery guarantees

`XREADGROUP` moves an event to the group's pending list. If a worker dies before `XACK`, another worker may `reclaim(..., 1000, "0-0", 8)` after the configured idle period. Use `next_cursor` to continue a bounded scan until it becomes `0-0`. Reclaiming **transfers ownership**, not execution authority: a slow old worker may still finish. Give each event a stable `message_id`, deduplicate side effects in your own transaction, and ACK **only after** that transaction commits.

For example, a worker should:
1. Poll or reclaim a batch.
2. Validate and parse each JSON body (Night does not automatically decode it).
3. Write the idempotent business result to the application's database, keyed by `message_id`.
4. ACK the Redis `stream_id`. Retry safely if ACK fails.

**Never perform a blocking or heavy Redis call inside an OW HTTP handler.** Night's current OW HTTP runtime is single-worker; run dedicated workers separately. Poll is non-blocking to avoid wedging an HTTP interpreter.

**Not guaranteed:** exactly-once processing, ordered processing across streams/consumer groups, automatic retries/backoff, poison-message dead lettering, durable PostgreSQL-outbox-to-stream dispatch, automatic workers, cluster failover, or Redis disk persistence. Redis ACKs depend on the Redis instance's persistence/failover settings. No automatic `MAXLEN` trimming is applied because trimming could drop **unacknowledged** messages; operators must explicitly manage retention and consumer-group lag.

## Verification

`NightRedisStreamsTest` includes strict argument validation, an actual Redis 7 publish/poll/ACK cycle and a simulated worker disconnect followed by `XAUTOCLAIM`. CI exercises this class in the existing real-Redis integration job. Passing CI is not production failover certification.

For Kafka producer/consumer groups, see [Night Kafka](kafka.md). Kafka and Redis Streams are separate transports; Night does not automatically bridge their events.
