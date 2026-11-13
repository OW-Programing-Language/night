package org.owlang.night.data;

import dev.oreo.Interpreter;
import java.util.List;

/**
 * PostgreSQL transactional outbox, with atomic SKIP LOCKED leasing.
 * An event must be enqueued inside the same JDBC transaction as business writes.
 * Delivery is at-least-once: consumers must handle duplicate event IDs.
 */
public final class NightOutbox {
    private final NightJdbc jdbc;
    public NightOutbox(NightJdbc jdbc) { this.jdbc = jdbc; }

    private static String validate(String value, int length, String name) {
        if (value == null || value.isBlank() || value.length() > length)
            throw new IllegalArgumentException("Invalid outbox " + name);
        return value;
    }

    /** Deduplicate by stable event ID, even after worker crashes. */
    public int enqueue(String eventId, String topic, String payload) {
        validate(eventId, 80, "event ID");
        validate(topic, 128, "topic");
        validate(payload, 1_000_000, "payload");
        return jdbc.execute("""
                INSERT INTO night_outbox(event_id, topic, payload)
                VALUES (?, ?, ?) ON CONFLICT(event_id) DO NOTHING
                """, List.of(eventId, topic, payload));
    }

    /** Lease one eligible event for a bounded duration; concurrent workers do not double-claim. */
    public List<Interpreter.OwRecord> claim(String worker, int leaseSeconds) {
        validate(worker, 128, "worker");
        if (leaseSeconds < 1 || leaseSeconds > 3600)
            throw new IllegalArgumentException("Lease seconds must be 1..3600");
        return jdbc.query("""
                UPDATE night_outbox SET state = 'LEASED',
                  attempts = attempts + 1,
                  lease_owner = ?,
                  lease_until = CURRENT_TIMESTAMP + (? * INTERVAL '1 second')
                WHERE event_id = (
                  SELECT event_id FROM night_outbox
                  WHERE ((state = 'PENDING' AND available_at <= CURRENT_TIMESTAMP)
                     OR (state = 'LEASED' AND lease_until < CURRENT_TIMESTAMP))
                  ORDER BY created_at, event_id
                  LIMIT 1 FOR UPDATE SKIP LOCKED
                )
                RETURNING event_id, topic, payload, attempts
                """, List.of(worker, leaseSeconds));
    }

    /** Only the current unexpired lease owner may acknowledge delivery. */
    public int acknowledge(String eventId, String worker) {
        validate(eventId, 80, "event ID");
        validate(worker, 128, "worker");
        return jdbc.execute("""
                UPDATE night_outbox SET state = 'DELIVERED',
                  delivered_at = CURRENT_TIMESTAMP, lease_owner = NULL, lease_until = NULL
                WHERE event_id = ? AND lease_owner = ? AND state = 'LEASED'
                  AND lease_until >= CURRENT_TIMESTAMP
                """, List.of(eventId, worker));
    }

    /** Schedule a retry, or mark a message dead after the configured attempt count. */
    public int fail(String eventId, String worker, int retrySeconds, int maxAttempts) {
        validate(eventId, 80, "event ID");
        validate(worker, 128, "worker");
        if (retrySeconds < 1 || retrySeconds > 86400 || maxAttempts < 1 || maxAttempts > 1000)
            throw new IllegalArgumentException("Invalid outbox retry policy");
        return jdbc.execute("""
                UPDATE night_outbox
                SET state = CASE WHEN attempts >= ? THEN 'DEAD' ELSE 'PENDING' END,
                    available_at = CURRENT_TIMESTAMP + (? * INTERVAL '1 second'),
                    lease_owner = NULL, lease_until = NULL
                WHERE event_id = ? AND lease_owner = ? AND state = 'LEASED'
                  AND lease_until >= CURRENT_TIMESTAMP
                """, List.of(maxAttempts, retrySeconds, eventId, worker));
    }
}
