# Night durable consumer inbox — Kafka/RabbitMQ (experimental)

Night now has **two distinct halves of delivery**:

1. **Outbox worker**: PostgreSQL business transaction → Kafka or RabbitMQ broker.
2. **Consumer worker**: Kafka/RabbitMQ broker → durable PostgreSQL **inbox** → OW application transaction.

The consumer worker never executes arbitrary OW handlers on broker threads. It persists a message in `night_inbox` and **only then** acknowledges Kafka's offset or RabbitMQ's delivery tag. Application logic processes rows independently by claiming an inbox lease. The application must commit its **business side effects and inbox ACK in one NightData transaction**.

This is **at least once**, not exactly once. A broker or worker crash after PostgreSQL insertion but before broker ACK causes redelivery; the unique `(inbox_namespace, topic, event_id)` key makes ingestion idempotent. If application side effects and inbox acknowledgement are not committed together, duplicate side effects are possible.

## 1. Run Flyway migrations

Run the existing production Flyway migrations including **`V6__night_consumer_inbox.sql`** and **`V7__night_broker_quarantine.sql`** before starting the consumer worker. A deployed worker deliberately does **not** migrate the database. The table contains `PENDING`, `LEASED`, `COMPLETED` and `DEAD` processing states.

A unique event ID is required on each message (`Kafka key` or `RabbitMQ message_id`, safe ASCII, 8–80 characters). Topics and namespaces must be safe ASCII identifiers. Malformed transport envelopes are saved to the [broker poison quarantine](broker-quarantine.md) before acknowledgement (missing/invalid ID, null body or oversized normal inbox body). Envelopes larger than the complete 1 MiB capture limit still fail closed without broker ACK.

## 2. Start a standalone broker consumer worker

```sh
export NIGHT_JDBC_URL='jdbc:postgresql://db.example.net/night?sslmode=verify-full'
export NIGHT_JDBC_USER=night_inbox_worker
export NIGHT_JDBC_PASSWORD='from-secret-manager'
export NIGHT_CONSUMER_NAMESPACE=orders_v1
export NIGHT_CONSUMER_IDLE_MS=250

# Kafka consumer (dedicated group/topic already provisioned):
export NIGHT_CONSUMER_BROKER=kafka
export NIGHT_CONSUMER_TOPIC=orders.created
export NIGHT_CONSUMER_GROUP=night_orders_v1
# Configure NIGHT_KAFKA_BOOTSTRAP_SERVERS and NIGHT_KAFKA_SECURITY_PROTOCOL
# as in kafka.md; SSL/SASL_SSL is required outside loopback testing.

java -cp "ow-lang/target/OwIDE.jar:platform/target/night-platform-0.2.0-SNAPSHOT.jar" \
  org.owlang.night.messaging.NightConsumerWorker
```

For RabbitMQ, configure `NIGHT_CONSUMER_BROKER=rabbitmq`, `NIGHT_RABBITMQ_URL=amqps://...`, and `NIGHT_RABBITMQ_QUEUE=night_events`; the queue name is stored as the inbox `topic`. Do not run Kafka and RabbitMQ consumers with the same namespace/topic/event-ID identity unless their event formats and idempotency semantics are intentionally shared.

Run the consumer independently of the web process under a supervisor such as the reference [systemd unit](../deploy/night-consumer.service) or Kubernetes. Unexpected DB/broker exceptions cause a **fail-fast exit**; the supervisor restarts the worker after backoff. A graceful signal attempts to finish the current intake before exit.

**Security and operations:** Use an ACL-limited broker identity, TLS certificate verification, a dedicated least-privilege PostgreSQL account, bounded worker replication, and alerts on restart loops. Kafka consumer-group offsets are committed manually after insert; RabbitMQ uses manual ACK. Both transports support PostgreSQL-backed quarantine of invalid envelopes, but no broker-side poison-message exchange or automatic application-schema validation. The worker does **not** choose or execute dynamic callbacks.

## 3. Process inbox events from OW

Import `src/modules/messaging/NightInbox.ow` and `NightData.ow`. The same Night platform extension provides both APIs over the **same** JDBC transaction context.

```ow
set inbox: new NightInbox()
set records: inbox.claim("orders_v1", "worker-app-1234", 60, 8)
# Process one returned event from records (if nonempty):
# 1. Parse/validate the payload.
# 2. Begin a NightData transaction.
# 3. Apply an idempotent business change via NightData.execute(...).
# 4. Call:
#    inbox.acknowledge("orders_v1", event.topic, event.event_id, "worker-app-1234")
# 5. Commit NightData. On any error: rollback, then optionally call fail().
```

In OW applications, use `try/catch/finally` or the appropriate structured cleanup provided by your OW compiler version to avoid leaving open transactions. Night also rolls back orphaned request transactions, but standalone workers must explicitly manage their own failures.

Available API (all bound by the Night platform extension):

| Method | Semantics |
|---|---|
| `claim(namespace, worker, lease_seconds, max_attempts)` | Bounded PostgreSQL SKIP LOCKED lease, up to one row |
| `acknowledge(namespace, topic, event_id, worker)` | Mark COMPLETED **inside the same SQL transaction** as business effects |
| `fail(namespace, topic, event_id, worker, retry_seconds, max_attempts)` | Schedule retry or move to DEAD |
| `replay_dead(namespace, topic, event_id)` | Operator-only explicit reset to PENDING |
| `reap_expired(namespace, max_attempts)` | Dead-letter exhausted crashed leases (called by claim automatically) |
| `accept(namespace, topic, event_id, json_body)` | Low-level idempotent insertion; bridge already calls this |

Worker IDs must be unique across running application processes, preferably randomized on every restart. If a worker exceeds its lease, its ACK fails; do **not** commit the business effect in that case. Retrying a failed event should use bounded backoff. **Only operators** should expose dead-letter replay and debugging interfaces; exposing them over a public HTTP route risks abusive reprocessing.

## 4. Failure model and verification

- **Crash after inbox commit, before broker ACK:** broker redelivers; `accept` returns duplicate without updating original payload or state; broker ACK proceeds.
- **Database unavailable:** broker ACK is never attempted, worker exits for supervised restart.
- **Malformed envelope:** upsert full bounded body and error reason into `night_broker_quarantine` before ACK. Oversized-to-retain messages are **not** ACKed.
- **Business transaction fails:** rollback both business work and the inbox ACK. Retry or move to durable DEAD after the attempt budget.
- **Application worker crashes after claim:** lease eventually expires and can be reclaimed, unless the configured attempt limit has been reached.
- **Dead letters:** manually review the row and call `replay_dead` after correcting the underlying problem. Completed events cannot be replayed by that API.

GitHub Actions run `NightInboxTest` and `NightConsumerBridgeTest` with PostgreSQL 16 and real RabbitMQ/Kafka brokers. This does not certify multi-node broker failover, multi-region consistency, exactly-once application effects, malicious payload parsing, or external security controls.

See [Night outbox worker](outbox-dispatcher.md), [Kafka](kafka.md), and [RabbitMQ](rabbitmq.md).

## Optional JSON/schema checks at intake

To validate message JSON before adding an event to the inbox, set `NIGHT_CONSUMER_SCHEMAS_FILE` to a trusted absolute path; run Flyway V8 first. The worker checks exact topic rules, refuses duplicate JSON object keys and routes malformed JSON or schema violations to the existing durable quarantine before broker ACK. Invalid or missing schema configuration fails closed. See [Night message schemas](message-schemas.md) for examples and limitations.
