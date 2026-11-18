# Night PostgreSQL outbox-to-broker dispatcher (experimental)

This module links the existing transactional OW `NightOutbox` to **one** external broker, either Kafka or RabbitMQ. It runs as a separate **supervised Java worker**, not on Night's single-worker HTTP server or within a request lifecycle. Enqueue events from OW in the **same JDBC transaction** as the business update, then let the worker publish them.

## Guarantees and limitations

- PostgreSQL `FOR UPDATE SKIP LOCKED` selects and leases a pending/expired event atomically, so parallel workers should not select the same active lease.
- A unique worker identity is generated at every process start. Only the unexpired lease owner can change the row to `DELIVERED` or schedule a retry.
- Kafka uses `acks=all`, idempotent producer configuration and broker metadata confirmation; RabbitMQ uses a durable queue, persistent messages and publisher confirmations.
- On failure, Night schedules an **exponential backoff** (bounded to `NIGHT_OUTBOX_MAX_RETRY_SECONDS`) and eventually marks the event `DEAD`. Expired leases that have exhausted the budget are also dead-lettered so repeated crashes cannot cause unbounded attempts.
- An administrator can explicitly requeue a `DEAD` event via `NightOutbox.replay_dead(event_id)` after investigating the failure. Never automatically replay dead letters forever.
- **At-least-once only:** if a broker accepts an event but the worker crashes, loses its lease or fails to ACK PostgreSQL, another worker can publish that same event again. Consumers **must deduplicate** using the stable `event_id` (the Kafka record key or RabbitMQ message ID) in the same transaction as their side effects. There is **no distributed Kafka/PostgreSQL transaction**.
- A long-running broker call exceeding its lease can cause concurrent publication. Configure the lease duration to exceed worst-case publisher latency plus recovery margin.

**Important routing constraint:** Every worker connected to a given `night_outbox` table must have **the same** `NIGHT_OUTBOX_BROKER` setting and destination policy. Do **not** run both Kafka and RabbitMQ workers against the same outbox table: their workers would *compete* for rows and split events rather than fan out to both. Separate databases/tables or add an explicit per-destination schema/lease policy before supporting fanout. RabbitMQ uses the single configured `NIGHT_RABBITMQ_QUEUE`, and the outbox `topic` is only logical metadata; Kafka uses the outbox `topic` as its pre-provisioned Kafka topic name.

This is a worker increment, **not** a full production-grade event platform: no autoscaling controller, transactional multi-broker fanout, schema registry, payload encryption, independent failover certification, dead-letter review dashboard or consumer-side exactly-once effects.

## Configuration

Install the Night Java platform and run Flyway migrations with the HTTP application or a separate deployment migration job *before* launching the worker. The worker will **not** silently migrate schema itself.

```sh
# Broker selector: exactly one value (must match for every worker using this DB)
export NIGHT_OUTBOX_BROKER=kafka  # or rabbitmq

# Standard Night PostgreSQL credentials (use TLS and a restricted database user)
export NIGHT_JDBC_URL='jdbc:postgresql://db.example.net:5432/night?sslmode=verify-full'
export NIGHT_JDBC_USER='night_worker'
export NIGHT_JDBC_PASSWORD='provided-by-a-secret-manager'

# Retry/lease settings (defaults shown)
export NIGHT_OUTBOX_LEASE_SECONDS=60
export NIGHT_OUTBOX_MAX_ATTEMPTS=8
export NIGHT_OUTBOX_RETRY_SECONDS=2
export NIGHT_OUTBOX_MAX_RETRY_SECONDS=300
export NIGHT_OUTBOX_IDLE_POLL_MS=500

# For Kafka, configure NIGHT_KAFKA_BOOTSTRAP_SERVERS and TLS/SASL as in kafka.md
# For RabbitMQ, configure NIGHT_RABBITMQ_URL=amqps://... and NIGHT_RABBITMQ_QUEUE as in rabbitmq.md
```

`leaseSeconds` must be 30–3600, max attempts 1–1000, retry seconds 1–86400, max retry >= base retry, and idle polling interval 100–5000 ms. All worker processes should use the same retry/max-attempt policy to avoid inconsistent lease reaping decisions. The worker processes one event per tick, then immediately checks the next; it sleeps when the outbox is idle. Supervise restart with backoff at the service manager rather than a tight local infinite exception loop.

## Running the standalone process

Build the pinned OW runtime and the shaded Night platform as in [production.md](production.md). Example for Linux/macOS, after successful migrations:

```sh
java -cp "ow-lang/target/OwIDE.jar:platform/target/night-platform-0.2.0-SNAPSHOT.jar" \
  org.owlang.night.messaging.NightOutboxWorker
```

Use the worker process independently of the web application. Prefer a restricted non-root service account and a supervisor, e.g. the [systemd template](../deploy/night-outbox.service). The process logs status and event ID only, not payloads. A termination signal stops the loop after the current tick; if a termination happens before ACK, the lease expiry permits recovery by another worker.

OW application enqueue example:

```ow
import "../../src/modules/data/NightOutbox.ow"
set outbox: new NightOutbox()

# After opening NightData and beginning the application's JDBC transaction:
call: outbox.enqueue("order-1001-created", "orders.created", "{\"orderId\":1001}")
# Commit the same NightData transaction as the business row changes.
```

Use unique event IDs. For Kafka, the logical topic must be a valid Kafka topic identifier provisioned and ACL-authorized in advance. For RabbitMQ the queue is fixed per worker. Broker credentials must be secured and rotated following an operator runbook.

## Failure injection and CI

`NightOutboxDispatcherTest` exercises PostgreSQL rollback/claim semantics, bounded retries, dead letters, explicit replay and two crash scenarios: a simulated broker outage and worker termination after publishing but before the outbox ACK. It also validates real RabbitMQ queue confirmation/consumption and real Kafka topic publication/consumption.

The PostgreSQL/RabbitMQ integration job and the separate Kafka+PostgreSQL integration job run these tests. CI does not test power failure, Kafka multi-node failover, broker certificate rotation or production worker SLOs. Run a staging soak/failover drill before relying on this for business-critical messages.

## Operational checklist

1. Apply schema migrations, prepare broker topic or durable queue and least-privilege identities.
2. Deploy **one** broker selector and identical retry policy per database/outbox table.
3. Run worker under a process supervisor and monitor worker restarts, `PENDING` lag, expired leases, `DEAD` rows and downstream duplicate rates.
4. Investigate dead letters, then explicitly replay only after the underlying error is corrected.
5. Rehearse worker crash immediately after broker confirmation and validate consumer idempotency.
