package org.owlang.night.messaging;

import java.util.Map;
import java.util.Objects;
import org.owlang.night.data.NightInbox;

/**
 * Optional, broker-native dead-letter notifications for durable quarantine.
 *
 * Only immutable metadata and a stable SHA-256 reference are sent to the
 * secondary topic/queue. The raw (possibly sensitive or oversized) message
 * stays in PostgreSQL's operator-restricted quarantine table.
 *
 * The bridge MUST durably capture the original envelope before calling us,
 * and may ACK the original delivery only after a successful broker confirm.
 * Duplicate notifications are possible if the ACK is lost; consumers dedup
 * using the stable fingerprint as their message ID.
 */
public final class NightPoisonDlq {
    private NightPoisonDlq() {}

    @FunctionalInterface
    public interface Publisher {
        void publish(NightConsumerBridge.Message source, String reason);
    }

    /**
     * Exact, explicit destination; no default, no implicit fanout.
     * Misconfiguration must fail before the ingestion worker opens brokers.
     */
    public static String configuredDestination(Map<String, String> env,
                                               String broker, String sourceTopic) {
        Objects.requireNonNull(env);
        NightInbox.topic(sourceTopic);
        String key = switch (broker) {
            case "kafka" -> "NIGHT_CONSUMER_DLQ_TOPIC";
            case "rabbitmq" -> "NIGHT_CONSUMER_DLQ_QUEUE";
            default -> throw new IllegalArgumentException("Unsupported dead-letter broker");
        };
        String other = "kafka".equals(broker)
                ? "NIGHT_CONSUMER_DLQ_QUEUE" : "NIGHT_CONSUMER_DLQ_TOPIC";
        if (env.containsKey(other) && !env.getOrDefault(other, "").isBlank())
            throw new IllegalArgumentException("Dead-letter destination for wrong broker");
        String destination = env.get(key);
        if (destination == null || destination.isBlank()) return null;
        NightInbox.topic(destination);
        if ("rabbitmq".equals(broker)
                && !destination.matches("[A-Za-z][A-Za-z0-9_.-]{0,63}"))
            throw new IllegalArgumentException("RabbitMQ DLQ name must be at most 64 characters");
        if (destination.equals(sourceTopic))
            throw new IllegalArgumentException("Dead-letter destination must differ from source");
        return destination;
    }

    /** Only safe, fixed-format metadata reaches the broker. */
    public static String referenceBody(NightConsumerBridge.Message event, String reason) {
        Objects.requireNonNull(event);
        NightInbox.topic(event.topic());
        if (!("INVALID_EVENT_ID".equals(reason) || "INVALID_PAYLOAD".equals(reason)
                || "PAYLOAD_TOO_LARGE".equals(reason)
                || "INVALID_JSON".equals(reason) || "SCHEMA_VIOLATION".equals(reason)))
            throw new IllegalArgumentException("Unknown dead-letter reason");
        if (!("kafka".equals(event.broker()) || "rabbitmq".equals(event.broker())))
            throw new IllegalArgumentException("Unknown broker");
        String hash = fingerprint(event);
        return "{\"version\":1,\"broker\":\"" + event.broker()
                + "\",\"source_topic\":\"" + event.topic()
                + "\",\"reason\":\"" + reason
                + "\",\"fingerprint\":\"" + hash + "\"}";
    }

    public static String fingerprint(NightConsumerBridge.Message event) {
        Objects.requireNonNull(event);
        return NightBrokerQuarantine.fingerprint(event.sourceIdentity());
    }

    /** Separate, TLS-configured producer instance from the source Kafka consumer. */
    public static Publisher kafka(NightKafka producer, String destination) {
        Objects.requireNonNull(producer);
        NightKafka.identifier(destination);
        return (event, reason) -> {
            if (!"kafka".equals(event.broker()))
                throw new IllegalArgumentException("Kafka DLQ cannot route another transport");
            if (destination.equals(event.topic()))
                throw new IllegalArgumentException("Kafka DLQ cannot be the source topic");
            String body = referenceBody(event, reason);
            producer.publish(destination, fingerprint(event), body);
        };
    }

    /** Independent RabbitMQ connection and durable queue with publisher confirms. */
    public static Publisher rabbitMq(NightRabbitMQ producer, String destination) {
        Objects.requireNonNull(producer);
        NightInbox.topic(destination);
        return (event, reason) -> {
            if (!"rabbitmq".equals(event.broker()))
                throw new IllegalArgumentException("RabbitMQ DLQ cannot route another transport");
            if (destination.equals(event.topic()))
                throw new IllegalArgumentException("RabbitMQ DLQ cannot be the source queue");
            String body = referenceBody(event, reason);
            if (producer.publishConfirmedTo(destination, fingerprint(event), body) != 1)
                throw new IllegalStateException("RabbitMQ dead-letter notification unconfirmed");
        };
    }
}
