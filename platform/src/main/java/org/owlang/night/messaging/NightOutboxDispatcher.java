package org.owlang.night.messaging;

import dev.oreo.Interpreter;
import java.util.List;
import java.util.Objects;
import org.owlang.night.data.NightOutbox;

/**
 * Single-event transactional outbox dispatcher. A separate worker process
 * must run this; never invoke it from an OW HTTP handler.
 *
 * Claims are atomic in PostgreSQL, broker publication must be confirmed before
 * ACK, and a crash after broker delivery can cause duplicate publication.
 * Consumers MUST deduplicate by the stable outbox event ID.
 */
public final class NightOutboxDispatcher {
    @FunctionalInterface
    public interface Publisher {
        void publish(String eventId, String topic, String payload);
    }

    public record Policy(int leaseSeconds, int maxAttempts,
                         int initialRetrySeconds, int maxRetrySeconds) {
        public Policy {
            if (leaseSeconds < 30 || leaseSeconds > 3600
                    || maxAttempts < 1 || maxAttempts > 1000
                    || initialRetrySeconds < 1 || initialRetrySeconds > 86400
                    || maxRetrySeconds < initialRetrySeconds || maxRetrySeconds > 86400)
                throw new IllegalArgumentException("Invalid Night outbox dispatcher policy");
        }

        /** Bounded exponential backoff, with no unbounded shifts/overflow. */
        public int backoffSeconds(int attempts) {
            if (attempts < 1)
                throw new IllegalArgumentException("Outbox attempt count must be positive");
            long retry = initialRetrySeconds;
            for (int i = 1; i < attempts && retry < maxRetrySeconds; i++)
                retry = Math.min(maxRetrySeconds, retry * 2L);
            return (int) retry;
        }
    }

    public record DispatchResult(String status, String eventId, int attempts) {}

    private final NightOutbox outbox;
    private final Publisher publisher;
    private final String workerId;
    private final Policy policy;

    public NightOutboxDispatcher(NightOutbox outbox, Publisher publisher,
                                 String workerId, Policy policy) {
        this.outbox = Objects.requireNonNull(outbox);
        this.publisher = Objects.requireNonNull(publisher);
        if (workerId == null || !workerId.matches("[A-Za-z0-9][A-Za-z0-9_-]{7,127}"))
            throw new IllegalArgumentException("Invalid outbox worker identity");
        this.workerId = workerId;
        this.policy = Objects.requireNonNull(policy);
    }

    /** Process at most one event. No unbounded loops or background threads. */
    public DispatchResult dispatchOne() {
        // A claim that timed out without fail() must also respect maxAttempts.
        outbox.deadLetterExhaustedLeases(policy.maxAttempts());
        List<Interpreter.OwRecord> next = outbox.claim(workerId, policy.leaseSeconds());
        if (next.isEmpty()) return new DispatchResult("IDLE", "", 0);

        Interpreter.OwRecord row = next.get(0);
        String eventId = (String) row.get("event_id");
        String topic = (String) row.get("topic");
        String payload = (String) row.get("payload");
        int attempts = ((Number) row.get("attempts")).intValue();

        try {
            // Confirm broker delivery BEFORE marking the outbox row delivered.
            publisher.publish(eventId, topic, payload);
        } catch (RuntimeException error) {
            // A failed confirmation may still mean the broker received it.
            // Retrying is at-least-once, not exactly once.
            int changed = outbox.fail(eventId, workerId,
                    policy.backoffSeconds(attempts), policy.maxAttempts());
            if (changed != 1)
                return new DispatchResult("LEASE_LOST", eventId, attempts);
            return new DispatchResult(attempts >= policy.maxAttempts()
                    ? "DEAD" : "RETRY", eventId, attempts);
        }

        // If lease expired during publication we must not falsely report ACK.
        int changed = outbox.acknowledge(eventId, workerId);
        return new DispatchResult(changed == 1 ? "DELIVERED" : "LEASE_LOST",
                eventId, attempts);
    }
}
