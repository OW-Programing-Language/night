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

The standalone [Night outbox dispatcher](outbox-dispatcher.md) now runs this workflow automatically using a dedicated Java process. It leases one event with `claim(worker-id, lease-seconds)`, confirms publish to **one configured** Kafka or RabbitMQ transport, then calls `acknowledge(event_id, worker-id)`. On failure it calls `fail(event_id, worker-id, retry-seconds, max-attempts)` using bounded exponential backoff. Events whose leases expire after the maximum number of attempts are dead-lettered. OW can still call the lower-level manual API, but one worker must never process the same event concurrently through a second transport.

## Guarantees

- PostgreSQL transaction durability when event insertion and business writes
  occur inside the same JDBC transaction.
- Unique caller-generated event ID prevents duplicate insertion.
- Atomic `SKIP LOCKED` leasing supports multiple polling workers.
- Acknowledgement and failure actions require the current, unexpired lease owner.
- Crash recovery makes an expired lease eligible for another worker.
- Exhausted attempts enter `DEAD` state. An operator can explicitly call `replay_dead(event_id)` to make that same event ID eligible for redelivery, reset attempts, and clear any lease; delivered events are never replayed by this method.

**At least once, not exactly once.** A worker can publish and crash before
acknowledgement. Downstream handlers must deduplicate on event ID.

## Limitations blocking 1.0 completion

- A dedicated broker worker loop now exists for **one configured transport** with bounded exponential backoff and manual replay. It does not implement a management UI, automatic dead-letter replay, jitter, broker-side idempotency, multi-destination fanout, or production rollout certification. Restrict `replay_dead` to operators.
- The claim operation requires PostgreSQL and must not be invoked with H2.
- Lease ownership is represented by worker name rather than a random fencing
  token; avoid sharing one worker ID between active processes.
- A slow publication exceeding its lease can be claimed again; consumers must
  be idempotent.
- Kafka and RabbitMQ end-to-end dispatcher integration tests are added in PR #12; production TLS/failover and throughput acceptance remain open. Redis Streams is not yet a dispatcher target.

This is an **increment**, not the completed roadmap outbox + replay + broker goal.

Night now also provides a separate [consumer inbox](inbox.md) for durable broker-to-PostgreSQL delivery and explicit application processing acknowledgements. It does not change the outbox producer's at-least-once guarantees or remove the need for business-level idempotency.
