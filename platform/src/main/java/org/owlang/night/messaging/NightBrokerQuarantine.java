package org.owlang.night.messaging;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import java.util.HexFormat;
import java.util.Objects;
import org.owlang.night.data.NightInbox;
import org.owlang.night.data.NightJdbc;

/**
 * Durable quarantine of invalid Kafka/RabbitMQ envelopes.
 *
 * Full payload and original ID are retained up to 1 MiB; above that limit the
 * worker FAILS CLOSED and does not acknowledge the broker. Only after this
 * PostgreSQL insert commits can a poison record's Kafka offset / Rabbit tag
 * be acknowledged. Access must be restricted to a separate operator role.
 */
public final class NightBrokerQuarantine {
    public static final int MAX_CAPTURE_BYTES = 1_048_576;
    private final NightJdbc jdbc;

    public NightBrokerQuarantine(NightJdbc jdbc) {
        this.jdbc = Objects.requireNonNull(jdbc);
    }

    static int utf8Size(String value) {
        if (value == null) return 0;
        if (value.length() > MAX_CAPTURE_BYTES * 3) return MAX_CAPTURE_BYTES * 3 + 1;
        return value.getBytes(StandardCharsets.UTF_8).length;
    }

    /** Hash the immutable broker identity rather than exposing it as SQL key. */
    public static String fingerprint(String source) {
        if (source == null || source.isBlank() || utf8Size(source) > MAX_CAPTURE_BYTES * 3)
            throw new IllegalArgumentException("Invalid poison broker identity");
        try {
            return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256")
                    .digest(source.getBytes(StandardCharsets.UTF_8)));
        } catch (NoSuchAlgorithmException ex) {
            throw new IllegalStateException("SHA-256 is unavailable", ex);
        }
    }

    public int capture(String namespace, String broker, String topic,
                       String identity, String reason, String eventId, String payload) {
        NightInbox.namespace(namespace);
        NightInbox.topic(topic);
        if (!"kafka".equals(broker) && !"rabbitmq".equals(broker))
            throw new IllegalArgumentException("Invalid poison broker");
        if (!("INVALID_EVENT_ID".equals(reason)
                || "INVALID_PAYLOAD".equals(reason)
                || "PAYLOAD_TOO_LARGE".equals(reason)))
            throw new IllegalArgumentException("Invalid poison reason");
        // Never partially retain data and then acknowledge a larger original.
        if (utf8Size(eventId) > MAX_CAPTURE_BYTES || utf8Size(payload) > MAX_CAPTURE_BYTES)
            throw new IllegalArgumentException("Poison envelope exceeds durable quarantine limit");
        return jdbc.execute("""
                INSERT INTO night_broker_quarantine
                (inbox_namespace, broker, topic, fingerprint, reason_code,
                 original_event_id, payload)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT (inbox_namespace, broker, topic, fingerprint)
                DO UPDATE SET redeliveries = night_broker_quarantine.redeliveries + 1,
                    last_seen = CURRENT_TIMESTAMP
                """, Arrays.asList(namespace, broker, topic, fingerprint(identity),
                reason, eventId, payload));
    }
}
