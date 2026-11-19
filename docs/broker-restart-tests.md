# Night broker restart and consumer handoff verification

Night's Kafka and RabbitMQ inbox bridges are designed for **at-least-once** delivery. The PostgreSQL inbox deduplicates by `(namespace, topic, event_id)`, broker ACK/offset commit occurs after durable storage, and a crash before acknowledgement should permit redelivery.

This milestone adds automated **real single-node broker restarts** and **consumer-group worker handoff** to the existing integration CI. It does **not** claim multi-node cluster failover or exactly-once side effects.

## What is actually exercised in GitHub Actions

### Kafka KRaft 3.9.1 — persistent topic and offsets

- A new topic, consumer group, and independent namespaced PostgreSQL inbox are created.
- Three distinct events are produced. Event 1 is committed to PostgreSQL and Kafka offset; event 2 is fetched but **intentionally not acknowledged**; event 3 remains queued.
- The first Java consumer process exits. CI **restarts the actual `night-kafka` container** using `docker restart`, preserving its storage.
- A second independent Java process verifies that events 2 and 3 are recovered once, event 1 is not reprocessed, and the committed offsets are durable. A third consumer start verifies the group remains idle.
- An additional Kafka test creates a second consumer in the same group after the first worker has a pending message; the first closes without committing, the second receives and commits it. A producer retry with the **same event ID** must result in `DUPLICATE` without modifying the original PostgreSQL payload.

### RabbitMQ 4 — durable queue and manual ACK

- Three persistent messages are published to a freshly declared durable RabbitMQ queue.
- Message 1 is persisted to PostgreSQL and manually ACKed. Message 2 is delivered but **left unacknowledged** when the connection closes; message 3 remains in the queue.
- CI restarts the real RabbitMQ service container. A separate Java process resumes the queue, persists and ACKs messages 2 and 3, verifies the single PostgreSQL inbox row for each ID and checks for an empty queue.

### Failure criteria and coverage boundaries

If any restored message is missing, if an acknowledged message replays, if a duplicate changes the original inbox payload, or if a broker fails to recover before timeout, CI fails. Both phases share the same PostgreSQL service and same broker container storage, but run in **different JVMs**. The synthetic run ID separates parallel CI runs.

This tests a broker process restart, not a failover to a different replica. It does not cover prolonged network partitions, leader elections in three-node Kafka KRaft, mirrored RabbitMQ quorum queues across nodes, TLS certificate rotation, full broker disk loss, downstream business transactions, or sustained throughput/latency acceptance. Those require a separate isolated staging cluster with fault injection, minimum in-sync replicas, realistic load, and audit logs.

## Local reproduction

The CI workflow provides the full orchestration. To run locally, start the broker and PostgreSQL as in `.github/workflows/kafka.yml` or `.github/workflows/production.yml`, export the corresponding `NIGHT_*` settings plus a numeric `NIGHT_FAILOVER_RUN_ID`, then execute:

```sh
# Kafka example:
mvn -B -f platform/pom.xml -Dtest=NightBrokerRestartTest -Dnight.failover.transport=kafka -Dnight.failover.phase=before test
docker restart night-kafka
# Wait until broker metadata API responds before the next command.
mvn -B -f platform/pom.xml -Dtest=NightBrokerRestartTest -Dnight.failover.transport=kafka -Dnight.failover.phase=after test

# RabbitMQ uses -Dnight.failover.transport=rabbitmq in both commands and
# a restart of its own RabbitMQ container between the invocations.
```

Do not rerun the `before` phase with the same run ID against the same database, topic and queue, because those fixtures are intentionally persisted between phases.

## Operations and remediation

- Retain the same broker group ID / queue and PostgreSQL namespace across worker restarts. Changing them may replay from the wrong offset or create a fresh inbox domain.
- Monitor the delay between publish and PostgreSQL insertion, dead-letter/quarantine rates, consumer lag, restart counts, PostgreSQL connection pool saturation and lost-ACK duplicates.
- Use broker-specific TLS/ACL identities and pre-provision Kafka replication factor, partition counts, min-ISR and RabbitMQ queue policies appropriate to the real topology.
- An unconfirmed publish can have reached the broker. Keep stable application event IDs and commit business effects together with the inbox ACK.
- Enable fail-fast supervision (systemd/Kubernetes) and avoid automatic destructive replay of quarantined payloads.

See [inbox](inbox.md), [dead-letter routing](dead-letter-routing.md), [outbox dispatcher](outbox-dispatcher.md) and [release gates](release-gates.md).
