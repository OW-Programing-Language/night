# Night Kafka producer and consumer groups (experimental)

Night provides an OW-native Kafka client adapter via `NightKafka.ow` and the optional Java platform runtime. It does **not** run Java APIs directly in OW source. Its goal is the minimum reliable messaging surface: idempotent producer with broker acknowledgements, manual consumer-group offsets, bounded polling and recovery after restart.

## Transport / authentication

Production **requires** `SSL` (default) or `SASL_SSL`. The Kafka cluster must expose properly configured TLS listeners, trust anchors and broker certificates.

```sh
export NIGHT_KAFKA_BOOTSTRAP_SERVERS='broker1.example.com:9093,broker2.example.com:9093'
export NIGHT_KAFKA_SECURITY_PROTOCOL='SSL'
# Optional when the Java system default CA store cannot validate the cluster:
export NIGHT_KAFKA_SSL_TRUSTSTORE_LOCATION='/path/to/truststore.jks'
export NIGHT_KAFKA_SSL_TRUSTSTORE_PASSWORD='from-secret-manager'
```

For `SASL_SSL`, set `NIGHT_KAFKA_SASL_MECHANISM` to `PLAIN`, `SCRAM-SHA-256`, or `SCRAM-SHA-512`; provide `NIGHT_KAFKA_SASL_JAAS_CONFIG` securely with the client login module and credentials. Treat JAAS and truststore passwords as secrets. The application never prints their values intentionally, but operators must protect process environments and broker logs.

**Plaintext is only permitted for integration tests** when `NIGHT_KAFKA_SECURITY_PROTOCOL=PLAINTEXT`, `NIGHT_KAFKA_ALLOW_INSECURE_LOOPBACK=true`, and *every* bootstrap endpoint is literal `localhost`, `127.0.0.1`, or `[::1]`. Do not use this bypass outside ephemeral local development/CI.

## OW API

```ow
import "../../src/modules/messaging/NightKafka.ow"

set kafka: new NightKafka()
call: kafka.open()
set receipt: kafka.publish("orders", "created-1001", "{\"orderId\":1001}")
# Receipt: {topic, partition, offset, message_id}

call: kafka.subscribe("orders", "order_handlers")
set event: kafka.poll(500)
print: event
# {present: 0} or {present: 1, topic, partition, offset, message_id, body}

# After validating the JSON and committing an idempotent database effect:
# call: kafka.acknowledge(event.topic, event.partition, event.offset, event.message_id)
# If the effect fails, call: kafka.retry_pending() or restart the worker.
```

- `open()` starts an idempotent producer, configured with `acks=all`, `enable.idempotence=true` and bounded send retries.
- `publish(topic, message_id, json_body)` returns broker metadata after confirmation or throws. Payloads are UTF-8 strings limited to 256 KiB. Kafka uses `message_id` as the Kafka record key. No automatic JSON decoding is performed.
- `subscribe(topic, group)` subscribes one instance to one topic/group, with `enable.auto.commit=false`. Each worker process should use its own runtime instance.
- `poll(wait_ms)` accepts 0–2000 milliseconds. It returns a single record, retaining it as pending until acknowledged. Repeating `poll` returns that same record until its ACK or retry.
- `acknowledge(topic, partition, offset, message_id)` commits **offset+1** for *only that partition* if and only if the parameters match the single pending event. A broker failure retains it pending.
- `retry_pending()` seeks to that event's offset (without committing), letting the application retry processing.

Topic/group names are restricted to safe ASCII identifiers beginning with a letter. Message IDs must contain 8–80 safe characters. **Topics must exist already**; automatic consumer-side topic creation is disabled. Provision topics and ACLs outside the OW application.

## Delivery and failure model

Kafka offsets are cumulative per partition. To avoid incorrectly skipping an earlier failed message, Night deliberately supports **one in-flight record per runtime Kafka consumer**, across all assigned partitions. This is slower than a normal high-throughput Kafka client but makes the manual OW API's acknowledgement behavior explicit.

**At-least-once, not exactly once:** A crash after the business transaction but before the Kafka offset commit will redeliver the event. Business logic must deduplicate on `message_id` in its own durable store. Publisher timeouts can also occur *after* an event is stored by Kafka; deduplicate retries. A producer is idempotent within its session, but this does not provide cross-process exactly-once business effects.

Kafka group rebalances and `max.poll.interval.ms` can revoke a partition if processing stalls. Such commits can fail and must be retried or recovered by another worker; the adapter intentionally does not claim consumer fencing. Run Kafka operations only in a supervised **dedicated worker**, never a web request handler. Validate event bodies before business actions.

No automatic outbox-to-Kafka worker, retry queues, DLQ, multiple-subscription management, partition ordering abstraction, schema registry, long-running polling loop, or production security/failover certification exists yet.

## Verification

`NightKafkaTest` covers fail-closed transport policy, invalid names/IDs and a **real Kafka 3.9.1 KRaft broker** for producer confirmations, manual offset commit, restart without replay after ACK, and redelivery after a simulated crash before ACK. The GitHub Actions `kafka-integration` job boots an ephemeral broker and runs this JUnit suite.

Passing CI is not a substitute for real TLS/SASL tests, multi-broker failover, long-running soak tests or independent release review.

For an independently supervised Kafka consumer that inserts into PostgreSQL **before** offset ACK, see [Night Inbox](inbox.md). This durable-inbox bridge does not implement arbitrary OW callbacks or broker-side poison-message quarantine.

The standalone consumer bridge can quarantine malformed Kafka envelope keys before committing their offsets; see [broker quarantine](broker-quarantine.md). Restart recovery is via the external supervisor, not uninterrupted multi-broker failover.

**Three-node KRaft regression:** the dedicated [multi-node HA lab](three-node-ha.md) provisions RF=3/min-ISR=2 topics, kills the actual topic leader and checks new leader election, acked messages, consumer offsets, and recovery when the failed node rejoins. It runs only against disposable localhost test brokers, not a certified production security topology.
