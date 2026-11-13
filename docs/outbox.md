# Night PostgreSQL transactional outbox (experimental)

Night provides an initial durable PostgreSQL outbox implementation in
`NightOutbox` (OW API) and `org.owlang.night.data.NightOutbox` (JDBC adapter).

## Schema and use

The sample production app's Flyway directory includes `V2__night_outbox.sql`.
Apply migrations before calling these APIs. The implementation currently
requires PostgreSQL; H2 is useful for basic migration checks but does not
support the PostgreSQL lease query.

```ow
import "../../src/modules/data/NightOutbox.ow"

# Inside an already-open NightData transaction:
set outbox: new NightOutbox()
call: outbox.enqueue("order-123-created", "orders.created", "{\"orderId\":123}")
# Commit NightData after both business state and outbox write succeed.
```

A separate worker can repeatedly call `claim("worker-id", 30)`, publish
the returned event to an external destination, then call
`acknowledge(event_id, "worker-id")`. A failed publication calls
`fail(event_id, "worker-id", retry_seconds, max_attempts)`.
The claim API returns at most one event.

## Guarantees

- PostgreSQL transaction durability when event insertion and business writes
  occur inside the same JDBC transaction.
- Unique caller-generated event ID prevents duplicate insertion.
- Atomic `SKIP LOCKED` leasing supports multiple polling workers.
- Acknowledgement and failure actions require the current, unexpired lease owner.
- Crash recovery makes an expired lease eligible for another worker.
- Exhausted attempts enter `DEAD` state for operator inspection and replay planning.

**At least once, not exactly once.** A worker can publish and crash before
acknowledgement. Downstream handlers must deduplicate on event ID.

## Limitations blocking 1.0 completion

- No built-in worker loop, broker adapter, schema registry, automated dead-letter
  replay tooling or exponential-backoff/jitter policy yet.
- The claim operation requires PostgreSQL and must not be invoked with H2.
- Lease ownership is represented by worker name rather than a random fencing
  token; avoid sharing one worker ID between active processes.
- A slow publication exceeding its lease can be claimed again; consumers must
  be idempotent.
- No end-to-end Kafka/RabbitMQ/Redis Stream delivery test exists yet.
- Test/CI code is committed, but live CI outcome needs verification.

This is an **increment**, not the completed roadmap outbox + replay + broker goal.
