package org.owlang.night.data;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.UUID;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

class NightInboxTest {
    private static boolean postgres() {
        return System.getenv().getOrDefault("NIGHT_JDBC_URL", "").startsWith("jdbc:postgresql:");
    }

    private static NightJdbc database() {
        NightJdbc jdbc = new NightJdbc();
        jdbc.open();
        Path migrations = Path.of("examples/production/db/migration");
        if (!Files.isDirectory(migrations))
            migrations = Path.of("../examples/production/db/migration");
        assertTrue(Files.isDirectory(migrations));
        jdbc.migrate("filesystem:" + migrations.toAbsolutePath());
        return jdbc;
    }

    private static String unique(String prefix) {
        return prefix + UUID.randomUUID().toString().replace("-", "");
    }

    private static String state(NightJdbc jdbc, String ns, String topic, String id) {
        var rows = jdbc.query("""
                SELECT state FROM night_inbox
                WHERE inbox_namespace=? AND topic=? AND event_id=?
                """, List.of(ns, topic, id));
        assertEquals(1, rows.size());
        return (String) rows.get(0).get("state");
    }

    @Test void rejectsUnsafeNamesWorkerIdsAndSizes() {
        assertEquals("group-1", NightInbox.namespace("group-1"));
        assertThrows(IllegalArgumentException.class, () -> NightInbox.namespace("../groups"));
        assertThrows(IllegalArgumentException.class, () -> NightInbox.topic("bad/topic"));
        assertThrows(IllegalArgumentException.class, () -> NightInbox.eventId("short"));
        try (NightJdbc jdbc = new NightJdbc()) {
            var inbox = new NightInbox(jdbc);
            assertThrows(IllegalArgumentException.class,
                    () -> inbox.accept("group", "topic", "event-12345678", "x".repeat(262145)));
            assertThrows(IllegalArgumentException.class,
                    () -> inbox.claim("group", "worker-1234", 0, 5));
            assertThrows(IllegalArgumentException.class,
                    () -> inbox.claim("group", "worker-1234", 30, 0));
            assertThrows(IllegalArgumentException.class,
                    () -> inbox.claim("group", "a", 30, 5));
        }
    }

    @Test void deduplicatesIntakeAndCommitsBusinessEffectWithAckAtomically() {
        Assumptions.assumeTrue(postgres());
        try (NightJdbc jdbc = database()) {
            var inbox = new NightInbox(jdbc);
            var outbox = new NightOutbox(jdbc);
            String ns = unique("test_");
            String id = unique("event-");
            String business = unique("business-");
            String topic = "orders.created";
            assertEquals(1, inbox.accept(ns, topic, id, "{\"id\":1}"));
            assertEquals(0, inbox.accept(ns, topic, id, "{\"id\":2}"));
            var claim = inbox.claim(ns, "worker-1234", 30, 4);
            assertEquals(1, claim.size());
            assertEquals(id, claim.get(0).get("event_id"));
            assertEquals("{\"id\":1}", claim.get(0).get("payload"));
            assertEquals(0, inbox.acknowledge(ns, topic, id, "wrong-worker"));

            jdbc.begin();
            assertEquals(1, outbox.enqueue(business, "orders.processed", "{}"));
            assertEquals(1, inbox.acknowledge(ns, topic, id, "worker-1234"));
            jdbc.rollback();
            assertEquals("LEASED", state(jdbc, ns, topic, id));
            assertEquals(0, jdbc.query("SELECT event_id FROM night_outbox WHERE event_id = ?",
                    List.of(business)).size());

            jdbc.begin();
            assertEquals(1, outbox.enqueue(business, "orders.processed", "{}"));
            assertEquals(1, inbox.acknowledge(ns, topic, id, "worker-1234"));
            jdbc.commit();
            assertEquals("COMPLETED", state(jdbc, ns, topic, id));
            assertEquals(1, jdbc.query("SELECT event_id FROM night_outbox WHERE event_id = ?",
                    List.of(business)).size());
            assertEquals(0, inbox.accept(ns, topic, id, "evil-replayed-payload"));
            assertEquals("COMPLETED", state(jdbc, ns, topic, id));
            assertTrue(inbox.claim(ns, "worker-1234", 30, 4).isEmpty());
            // Keep the shared CI outbox table clean for the existing
            // dispatcher integration suite (which leases the oldest event).
            assertEquals(1, jdbc.execute(
                    "DELETE FROM night_outbox WHERE event_id = ?", List.of(business)));
        }
    }

    @Test void retriesWithBackoffThenDeadLettersAndReplaysOnDemand() throws Exception {
        Assumptions.assumeTrue(postgres());
        try (NightJdbc jdbc = database()) {
            var inbox = new NightInbox(jdbc);
            String ns = unique("retry_");
            String id = unique("event-");
            assertEquals(1, inbox.accept(ns, "payments.received", id, "{}"));
            assertEquals(1, inbox.claim(ns, "worker-1234", 30, 2).size());
            assertEquals(1, inbox.fail(ns, "payments.received", id, "worker-1234", 1, 2));
            assertEquals("PENDING", state(jdbc, ns, "payments.received", id));
            assertTrue(inbox.claim(ns, "worker-1234", 30, 2).isEmpty());
            Thread.sleep(1200);
            assertEquals(2, inbox.claim(ns, "worker-1234", 30, 2)
                    .get(0).get("attempts"));
            assertEquals(1, inbox.fail(ns, "payments.received", id, "worker-1234", 1, 2));
            assertEquals("DEAD", state(jdbc, ns, "payments.received", id));
            assertTrue(inbox.claim(ns, "worker-1234", 30, 2).isEmpty());
            assertEquals(0, inbox.accept(ns, "payments.received", id, "different"));
            assertEquals("DEAD", state(jdbc, ns, "payments.received", id));
            assertEquals(1, inbox.replayDead(ns, "payments.received", id));
            assertEquals(0, inbox.replayDead(ns, "payments.received", id));
            assertEquals(1, inbox.claim(ns, "worker-5678", 30, 2).size());
            assertEquals(1, inbox.acknowledge(ns, "payments.received", id, "worker-5678"));
            assertEquals("COMPLETED", state(jdbc, ns, "payments.received", id));
            assertEquals(0, inbox.replayDead(ns, "payments.received", id));
        }
    }

    @Test void expiresCrashedLeaseAndRespectsDeadLetterBudget() throws Exception {
        Assumptions.assumeTrue(postgres());
        try (NightJdbc jdbc = database()) {
            var inbox = new NightInbox(jdbc);
            String ns = unique("crash_");
            String id = unique("event-");
            inbox.accept(ns, "orders.created", id, "{}");
            assertEquals(1, inbox.claim(ns, "worker-1234", 1, 1).size());
            Thread.sleep(1200);
            assertEquals(1, inbox.deadLetterExpired(ns, 1));
            assertEquals("DEAD", state(jdbc, ns, "orders.created", id));
            assertEquals(0, inbox.acknowledge(ns, "orders.created", id, "worker-1234"));
            assertTrue(inbox.claim(ns, "worker-5678", 1, 1).isEmpty());
            assertEquals(1, inbox.replayDead(ns, "orders.created", id));
            assertEquals(1, inbox.claim(ns, "worker-5678", 30, 1).size());
            assertEquals(1, inbox.acknowledge(ns, "orders.created", id, "worker-5678"));
        }
    }
}
