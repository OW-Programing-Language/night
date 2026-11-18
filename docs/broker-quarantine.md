# Night broker poison-envelope quarantine (experimental)

Night's standalone [consumer inbox worker](inbox.md) now persists **malformed Kafka and RabbitMQ envelopes** in PostgreSQL before advancing a Kafka group offset or ACKing an AMQP delivery. This avoids one missing event ID blocking an entire consumer group or queue indefinitely.

This milestone quarantines **transport-envelope violations** only: missing/invalid event IDs, null payloads, and payloads exceeding the normal **256 KiB** inbox limit. Night does **not** interpret JSON schemas or automatically detect business-level poison messages; that work remains in the OW application or a future schema-aware consumer.

## Migration / storage

Apply Flyway `V7__night_broker_quarantine.sql` before upgrading the consumer worker. The `night_broker_quarantine` table records:

- `inbox_namespace`, `broker` (`kafka` or `rabbitmq`), `topic`, and a SHA-256 `fingerprint` of the broker message identity
- Fixed `reason_code`: `INVALID_EVENT_ID`, `INVALID_PAYLOAD`, or `PAYLOAD_TOO_LARGE`
- `original_event_id`, `payload` (nullable), first/last-seen timestamps, and redelivery count

**Data protection:** Quarantine contains the original message body and may contain sensitive information. Restrict SELECT access to trained operators, implement retention and backup encryption, and do not expose it through public web routes or application metrics. Runtime worker logs should report only counts/statuses, not payloads.

**Size policy:** To prevent acknowledgement after silently truncating data, quarantined event IDs and payloads are stored in full up to **1 MiB UTF-8 each**. Larger envelopes fail closed: the worker exits **without ACK**, so an operator must remediate the oversized message or enforce broker-side message limits. This is intentionally not a silent message discard.

## Acknowledgement ordering

```text
Broker event → validate envelope
                  ├─ valid → night_inbox INSERT/ON CONFLICT → COMMIT → broker ACK
                  └─ invalid → quarantine INSERT/UPSERT → COMMIT → broker ACK
```

If PostgreSQL is down, migrations are missing, or quarantine storage fails, the broker offset/tag is **not acknowledged**. If the process crashes after quarantine commits but before broker ACK, delivery repeats; the quarantine row is upserted and `redeliveries` increments, preserving its original contents.

- **Kafka:** fingerprint derives from the immutable topic/partition/offset. The internal ingestion API can ACK even a *null Kafka key* without weakening OW's original strict `poll()` validation.
- **RabbitMQ:** delivery tags can change across reconnects. A fingerprint of the queue, original event ID, and full body deduplicates repeated *identical* poison envelopes. Distinct messages with identical content may intentionally share a quarantine row; their `redeliveries` count is not an exact delivery count across distinct identical messages.

The worker still requires TLS/SASL or the explicitly configured loopback CI exception in its native Kafka/RabbitMQ adapters. Broker interruptions remain **fail-fast**: use `Restart=on-failure` / Kubernetes supervision to reopen the connection, reconnect to the consumer group, and safely redeliver unacknowledged messages. This is **process-level restart recovery**, not uninterrupted automatic in-process reconnection or multi-node HA certification.

## Operator recovery

Query the table using a restricted administrative connection:

```sql
SELECT inbox_namespace, broker, topic, reason_code,
       fingerprint, redeliveries, first_seen, last_seen
FROM night_broker_quarantine
ORDER BY last_seen DESC
LIMIT 50;
```

Inspect raw payloads only through an authorized, audited operational process. This feature **does not automatically replay** invalid messages. A corrected envelope must be republished with an appropriate safe event ID by an authorized operator; keep the original fingerprint for the incident record. Retain quarantine rows until the retention policy and operational review allow deletion.

Do not directly edit Kafka offsets or force ACKs when PostgreSQL persistence has failed. Set broker-level maximum message sizes aligned with Night's limits to prevent permanently blocking workers.

## Validation evidence / limitations

Real PostgreSQL and broker integration tests cover missing Kafka keys, missing RabbitMQ message IDs, subsequent valid-message progress, unacknowledged database failures, duplicate redelivery after lost ACK, and reconnection via a new consumer process. They do not certify a real multi-node Kafka/RabbitMQ cluster, network partition healing, broker certificate rotation, business payload schema validation, recovery at scale, or throughput SLOs. Those items remain unchecked in [the 1.0 roadmap](https://github.com/OW-Programing-Language/night/issues/3).
