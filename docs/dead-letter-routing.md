# Broker dead-letter reference routing (experimental)

Night's standalone Kafka/RabbitMQ consumer can optionally publish a **confirmed, durable dead-letter reference** after writing an invalid envelope to the PostgreSQL quarantine table, but **before acknowledging** the original broker delivery. This is an application-managed DLQ notification, **not** RabbitMQ's built-in dead-letter exchange (DLX) or a Kafka transaction.

This feature builds on [quarantine](broker-quarantine.md) and [per-topic JSON field rules](message-schemas.md). The quarantine table is still the authoritative store for the **original, complete payload**. DLQ notifications carry only safe fixed-format metadata, so raw data and credentials are not duplicated into a second broker destination.

## Opt-in configuration

For Kafka consumers, configure a **pre-provisioned and ACL-authorized** dead-letter topic:

```sh
export NIGHT_CONSUMER_BROKER=kafka
export NIGHT_CONSUMER_TOPIC=orders.created
export NIGHT_CONSUMER_GROUP=order_workers
export NIGHT_CONSUMER_NAMESPACE=orders_v1
export NIGHT_CONSUMER_DLQ_TOPIC=night_poison_events
```

For RabbitMQ consumers, configure a **separate durable queue**, declared automatically by the Night DLQ publisher:

```sh
export NIGHT_CONSUMER_BROKER=rabbitmq
export NIGHT_RABBITMQ_QUEUE=night_events
export NIGHT_CONSUMER_NAMESPACE=orders_v1
export NIGHT_CONSUMER_DLQ_QUEUE=night_poison_events
```

If the destination setting is absent or empty, Night keeps the existing PostgreSQL-only quarantine behavior (no broker DLQ publishing). Any destination equal to the source topic/queue, containing unsafe characters, or configured for the wrong broker **fails startup**. A DLQ producer uses a **separate connection** and the same TLS/ACL protections as the configured broker adapter. Never subscribe the normal application consumer to its own DLQ topic or queue.

The producer emits one compact JSON reference:

```json
{
  "version": 1,
  "broker": "kafka",
  "source_topic": "orders.created",
  "reason": "SCHEMA_VIOLATION",
  "fingerprint": "64_lowercase_sha256_hex_characters"
}
```

The Kafka **record key** or RabbitMQ **message ID** is the exact same 64-character SHA-256 fingerprint as the PostgreSQL `night_broker_quarantine.fingerprint` column. The metadata payload does **not** contain the rejected event's original body, invalid message ID or secrets.

## Safety and recovery

```text
Invalid broker envelope
  → validate transport fields / optional Night schema rules
  → upsert full bounded envelope in PostgreSQL quarantine (committed)
  → publish stable reference to separate DLQ topic/queue (broker confirm)
  → ACK/commit original source delivery
```

If PostgreSQL is unavailable, the quarantine insert fails, the DLQ is **not** published, and the source stays unacknowledged. If DLQ publishing fails or its broker confirmation times out, the original source also stays unacknowledged; the worker exits for supervised restart. If the process crashes after DLQ confirmation but before source ACK, the same source may be delivered again: both quarantine `redeliveries` and broker DLQ notifications can repeat. **Deduplicate DLQ notifications by fingerprint.** A Kafka idempotent producer does not provide transactional exactly-once processing across PostgreSQL and the source group.

For Kafka the quarantine identity includes source topic, partition and offset. For RabbitMQ an immutable content-derived hash is used because delivery tags change during reconnection; identical poison envelopes may collapse into the same quarantine record.

**Size policy:** Night never truncates original poison bodies to publish DLQ. A message too large for full quarantine retention remains unacknowledged and cannot generate a reference notification. Configure broker limits to prevent large-message loops.

## Operator procedure

1. Run Flyway migrations up through V7 for transport-envelope quarantine and V8 for optional schema-aware validation.
2. Create the destination topic/queue and grant the **consumer's DLQ producer credentials** appropriate publish permissions, without granting ordinary application consumers quarantine database access.
3. Monitor the DLQ for fingerprints, then inspect the matching restricted PostgreSQL quarantine row for the complete payload and error reason.
4. Correct and republish a safe event manually with a stable *application* event ID, and record the original quarantine fingerprint for audit.
5. Apply a clear DLQ retention policy (and database retention policy) rather than replaying invalid messages automatically.

## Remaining limitations

This is opt-in, application-managed dead-letter **reference routing**, not native RabbitMQ DLX / dead-letter policy configuration, full DLQ payload mirroring, JSON Schema registry integration, automatic poison replay, Kafka transactions, consumer fencing, or multi-node TLS/broker failover certification. Reference topic/queue setup and access controls remain operator responsibilities.

Regression coverage uses real Kafka KRaft and RabbitMQ plus PostgreSQL, including confirm-before-source-ACK and a simulated DLQ publish outage that leaves the source unacknowledged.
