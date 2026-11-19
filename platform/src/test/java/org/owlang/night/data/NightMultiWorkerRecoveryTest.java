package org.owlang.night.data;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

/** Tests real PostgreSQL SKIP LOCKED fencing across independent JDBC workers. */
class NightMultiWorkerRecoveryTest {
    private static final String TOPIC = "orders.created";

    private static boolean postgres() {
        return System.getenv().getOrDefault("NIGHT_JDBC_URL", "").startsWith("jdbc:postgresql:");
    }

    private static String unique(String prefix) {
        return prefix + UUID.randomUUID().toString().replace("-", "");
    }

    private static NightJdbc database() {
        NightJdbc jdbc = new NightJdbc();
        jdbc.open();
        return jdbc;
    }

    private static void migrate(NightJdbc jdbc) {
        Path migrations = Path.of("examples/production/db/migration");
        if (!Files.isDirectory(migrations))
            migrations = Path.of("../examples/production/db/migration");
        assertTrue(Files.isDirectory(migrations));
        jdbc.migrate("filesystem:" + migrations.toAbsolutePath());
    }

    @Test void independentWorkersNeverClaimOrAcknowledgeTheSameEventTwice() throws Exception {
        Assumptions.assumeTrue(postgres());
        String namespace = unique("parallel_");
        int eventCount = 16;
        int workerCount = 4;

        try (NightJdbc setup = database()) {
            migrate(setup);
            NightInbox inbox = new NightInbox(setup);
            for (int i = 0; i < eventCount; i++)
                assertEquals(1, inbox.accept(namespace, TOPIC,
                        "event-" + namespace + "-" + i, "{\"ordinal\":" + i + "}"));
        }

        Set<String> seen = ConcurrentHashMap.newKeySet();
        AtomicInteger deliveries = new AtomicInteger();
        CountDownLatch ready = new CountDownLatch(workerCount);
        CountDownLatch start = new CountDownLatch(1);
        ExecutorService executor = Executors.newFixedThreadPool(workerCount);
        try {
            List<Callable<Void>> workers = java.util.stream.IntStream.range(0, workerCount)
                    .<Callable<Void>>mapToObj(index -> () -> {
                        String worker = "worker-parallel-" + index;
                        try (NightJdbc jdbc = database()) {
                            NightInbox inbox = new NightInbox(jdbc);
                            ready.countDown();
                            assertTrue(start.await(10, TimeUnit.SECONDS));
                            // A worker can see no available records while
                            // another owns the final lease; that is expected.
                            for (int attempt = 0; attempt < eventCount + 2; attempt++) {
                                var claimed = inbox.claim(namespace, worker, 30, 3);
                                if (claimed.isEmpty()) break;
                                String eventId = (String) claimed.get(0).get("event_id");
                                assertTrue(seen.add(eventId), "Duplicate active lease: " + eventId);
                                deliveries.incrementAndGet();
                                assertEquals(0, inbox.acknowledge(namespace, TOPIC, eventId,
                                        "wrong-worker-id"));
                                assertEquals(1, inbox.acknowledge(namespace, TOPIC, eventId, worker));
                            }
                        }
                        return null;
                    }).toList();
            List<Future<Void>> results = workers.stream().map(executor::submit).toList();
            assertTrue(ready.await(10, TimeUnit.SECONDS));
            start.countDown();
            for (Future<Void> result : results) result.get(30, TimeUnit.SECONDS);
        } finally {
            start.countDown();
            executor.shutdownNow();
            assertTrue(executor.awaitTermination(10, TimeUnit.SECONDS));
        }

        assertEquals(eventCount, deliveries.get());
        assertEquals(eventCount, seen.size());
        try (NightJdbc verification = database()) {
            var grouped = verification.query("""
                    SELECT state, COUNT(*) AS total FROM night_inbox
                     WHERE inbox_namespace = ? GROUP BY state
                    """, List.of(namespace));
            assertEquals(1, grouped.size());
            assertEquals("COMPLETED", grouped.get(0).get("state"));
            assertEquals(eventCount, ((Number) grouped.get(0).get("total")).intValue());
        }
    }

    @Test void anotherJdbcProcessReclaimsExpiredLeaseWhileStaleWorkerCannotAck() throws Exception {
        Assumptions.assumeTrue(postgres());
        String namespace = unique("worker_crash_");
        String id = "event-" + namespace;
        try (NightJdbc first = database(); NightJdbc other = database()) {
            migrate(first);
            NightInbox firstInbox = new NightInbox(first);
            NightInbox secondInbox = new NightInbox(other);
            firstInbox.accept(namespace, TOPIC, id, "{}");
            assertEquals(1, firstInbox.claim(namespace, "worker-first-01", 1, 3).size());
            assertTrue(secondInbox.claim(namespace, "worker-second-02", 1, 3).isEmpty());
            Thread.sleep(1250);
            var redelivered = secondInbox.claim(namespace, "worker-second-02", 30, 3);
            assertEquals(1, redelivered.size());
            assertEquals(id, redelivered.get(0).get("event_id"));
            assertEquals(2, ((Number) redelivered.get(0).get("attempts")).intValue());
            assertEquals(0, firstInbox.acknowledge(namespace, TOPIC, id, "worker-first-01"));
            assertEquals(1, secondInbox.acknowledge(namespace, TOPIC, id, "worker-second-02"));
            assertTrue(firstInbox.claim(namespace, "worker-first-01", 1, 3).isEmpty());
        }
    }
}
