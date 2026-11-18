package org.owlang.night.data;

import dev.oreo.Interpreter;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Objects;

/**
 * Durable, idempotent broker intake and separately leased application inbox.
 *
 * A consumer must persist an event through accept() BEFORE acknowledging the
 * broker. OW application processing must commit its business transaction and
 * acknowledge() on the SAME NightJdbc transaction to avoid double effects.
 * Duplicate broker deliveries are expected under at-least-once semantics.
 */
public final class NightInbox {
    private final NightJdbc jdbc;
    private static final int MAX_PAYLOAD_BYTES = 262_144;

    public NightInbox(NightJdbc jdbc) {
        this.jdbc = Objects.requireNonNull(jdbc);
    }

    public static String namespace(String raw) {
        if (raw == null || !raw.matches("[A-Za-z][A-Za-z0-9_.-]{0,63}"))
            throw new IllegalArgumentException("Invalid Night inbox namespace");
        return raw;
    }

    public static String topic(String raw) {
        if (raw == null || !raw.matches("[A-Za-z][A-Za-z0-9_.-]{0,127}"))
            throw new IllegalArgumentException("Invalid Night inbox topic");
        return raw;
    }

    public static String eventId(String raw) {
        if (raw == null || !raw.matches("[A-Za-z0-9_-]{8,80}"))
            throw new IllegalArgumentException("Invalid Night inbox event ID");
        return raw;
    }

    private static String worker(String raw) {
        if (raw == null || !raw.matches("[A-Za-z0-9][A-Za-z0-9_-]{7,127}"))
            throw new IllegalArgumentException("Invalid Night inbox worker ID");
        return raw;
    }

    private static int attempts(int limit) {
        if (limit < 1 || limit > 1000)
            throw new IllegalArgumentException("Inbox max attempts must be 1..1000");
        return limit;
    }

    /**
     * Returns 1 on first insert, 0 for an already seen (namespace/topic/id).
     * Do not update payload or state on duplicate: completed/dead items remain
     * completed/dead even when the same Kafka/RabbitMQ event is redelivered.
     */
    public int accept(String namespace, String topic, String eventId, String payload) {
        namespace(namespace);
        topic(topic);
        eventId(eventId);
        if (payload == null || payload.getBytes(StandardCharsets.UTF_8).length > MAX_PAYLOAD_BYTES)
            throw new IllegalArgumentException("Invalid Night inbox payload");
        return jdbc.execute("""
                INSERT INTO night_inbox(inbox_namespace, topic, event_id, payload)
                VALUES (?, ?, ?, ?)
                ON CONFLICT (inbox_namespace, topic, event_id) DO NOTHING
                """, List.of(namespace, topic, eventId, payload));
    }

    /** Preserve a bounded redelivery budget when a worker crashes repeatedly. */
    public int deadLetterExpired(String namespace, int maxAttempts) {
        namespace(namespace);
        attempts(maxAttempts);
        return jdbc.execute("""
                UPDATE night_inbox SET state = 'DEAD',
                    lease_owner = NULL, lease_until = NULL
                WHERE inbox_namespace = ? AND state = 'LEASED'
                  AND lease_until < CURRENT_TIMESTAMP AND attempts >= ?
                """, List.of(namespace, maxAttempts));
    }

    /**
     * Lease a single eligible record. This does not execute business logic.
     * Multiple OW processes may claim different records via SKIP LOCKED.
     */
    public List<Interpreter.OwRecord> claim(String namespace, String worker,
                                            int leaseSeconds, int maxAttempts) {
        namespace(namespace);
        worker(worker);
        attempts(maxAttempts);
        if (leaseSeconds < 1 || leaseSeconds > 3600)
            throw new IllegalArgumentException("Inbox lease seconds must be 1..3600");
        deadLetterExpired(namespace, maxAttempts);
        return jdbc.query("""
                UPDATE night_inbox SET state = 'LEASED',
                    attempts = attempts + 1,
                    lease_owner = ?,
                    lease_until = CURRENT_TIMESTAMP + (? * INTERVAL '1 second')
                WHERE (inbox_namespace, topic, event_id) IN (
                    SELECT inbox_namespace, topic, event_id FROM night_inbox
                    WHERE inbox_namespace = ? AND attempts < ?
                      AND ((state = 'PENDING' AND available_at <= CURRENT_TIMESTAMP)
                        OR (state = 'LEASED' AND lease_until < CURRENT_TIMESTAMP))
                    ORDER BY received_at, topic, event_id LIMIT 1 FOR UPDATE SKIP LOCKED
                )
                RETURNING inbox_namespace, topic, event_id, payload, attempts
                """, List.of(worker, leaseSeconds, namespace, maxAttempts));
    }

    /** Call inside the same JDBC transaction as the application's business writes. */
    public int acknowledge(String namespace, String topic, String eventId, String worker) {
        namespace(namespace);
        topic(topic);
        eventId(eventId);
        worker(worker);
        return jdbc.execute("""
                UPDATE night_inbox SET state = 'COMPLETED',
                    completed_at = CURRENT_TIMESTAMP, lease_owner = NULL, lease_until = NULL
                WHERE inbox_namespace = ? AND topic = ? AND event_id = ?
                  AND state = 'LEASED' AND lease_owner = ?
                  AND lease_until >= CURRENT_TIMESTAMP
                """, List.of(namespace, topic, eventId, worker));
    }

    /** Schedule a bounded retry, or move an exhausted item into durable DEAD state. */
    public int fail(String namespace, String topic, String eventId, String worker,
                    int retrySeconds, int maxAttempts) {
        namespace(namespace);
        topic(topic);
        eventId(eventId);
        worker(worker);
        attempts(maxAttempts);
        if (retrySeconds < 1 || retrySeconds > 86400)
            throw new IllegalArgumentException("Inbox retry seconds must be 1..86400");
        return jdbc.execute("""
                UPDATE night_inbox
                   SET state = CASE WHEN attempts >= ? THEN 'DEAD' ELSE 'PENDING' END,
                       available_at = CURRENT_TIMESTAMP + (? * INTERVAL '1 second'),
                       lease_owner = NULL, lease_until = NULL
                 WHERE inbox_namespace = ? AND topic = ? AND event_id = ?
                   AND state = 'LEASED' AND lease_owner = ?
                   AND lease_until >= CURRENT_TIMESTAMP
                """, List.of(maxAttempts, retrySeconds, namespace, topic, eventId, worker));
    }

    /** Operator-only dead-letter replay: preserves stable ID and original payload. */
    public int replayDead(String namespace, String topic, String eventId) {
        namespace(namespace);
        topic(topic);
        eventId(eventId);
        return jdbc.execute("""
                UPDATE night_inbox
                   SET state = 'PENDING', attempts = 0, available_at = CURRENT_TIMESTAMP,
                       lease_owner = NULL, lease_until = NULL
                 WHERE inbox_namespace = ? AND topic = ? AND event_id = ? AND state = 'DEAD'
                """, List.of(namespace, topic, eventId));
    }
}
