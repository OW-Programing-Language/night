package org.owlang.night.messaging;

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.TopicDescription;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Test;
import org.owlang.night.data.NightInbox;
import org.owlang.night.data.NightJdbc;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Three-node Kafka/KRaft CI fault injection, not production TLS/host HA.
 *
 * Separate JVM phases surround a kill of the actual partition leader,
 * preserving the other two broker+controller processes and PostgreSQL.
 */
class NightKafkaQuorumFailoverTest {
    private static boolean phase(String value) {
        return value.equals(System.getProperty("night.ha.phase"));
    }

    private static String runId() {
        String id = System.getenv("NIGHT_HA_RUN_ID");
        if (id == null || !id.matches("[0-9]{1,20}"))
            throw new IllegalStateException("NIGHT_HA_RUN_ID must be CI numeric");
        return id;
    }

    private static String topic() { return "night_ha_" + runId(); }
    private static String group() { return "night_ha_group_" + runId(); }
    private static String namespace() { return "night_ha_inbox_" + runId(); }
    private static String event(int index) { return "event-" + runId() + "-" + index; }

    private static NightJdbc database() {
        NightJdbc jdbc = new NightJdbc();
        jdbc.open();
        Path migrations = Path.of("examples/production/db/migration");
        if (!Files.isDirectory(migrations)) migrations = Path.of("../examples/production/db/migration");
        assertTrue(Files.isDirectory(migrations), "Missing Night SQL migrations");
        jdbc.migrate("filesystem:" + migrations.toAbsolutePath());
        return jdbc;
    }

    private static List<String> storedIds(NightJdbc jdbc) {
        return jdbc.query("SELECT event_id FROM night_inbox WHERE inbox_namespace=? ORDER BY event_id",
                List.of(namespace())).stream().map(x -> (String) x.get("event_id")).toList();
    }

    private static int awaitReplicas(int expected) throws Exception {
        try (AdminClient admin = AdminClient.create(NightKafka.config(System.getenv()))) {
            int leader = -1;
            for (int i = 0; i < 90; i++) {
                try {
                    TopicDescription description = admin.describeTopics(List.of(topic()))
                            .allTopicNames().get(15, TimeUnit.SECONDS).get(topic());
                    var partitions = description.partitions();
                    assertEquals(1, partitions.size());
                    var partition = partitions.get(0);
                    if (partition.leader() != null) leader = partition.leader().id();
                    if (partition.isr().size() == expected && leader > 0) return leader;
                } catch (java.util.concurrent.ExecutionException ex) {
                    // KRaft metadata propagates asynchronously after topic
                    // creation or leader election; only these transient
                    // broker errors are retried, with a strict 90s cap.
                    Throwable cause = ex.getCause();
                    if (!(cause instanceof org.apache.kafka.common.errors.UnknownTopicOrPartitionException)
                            && !(cause instanceof org.apache.kafka.common.errors.LeaderNotAvailableException))
                        throw ex;
                }
                Thread.sleep(1000);
            }
            fail("Kafka topic did not reach " + expected + " in-sync replicas");
            return -1;
        }
    }

    private static NightConsumerBridge.Result await(NightConsumerBridge bridge) {
        for (int i = 0; i < 60; i++) {
            var result = bridge.consumeOne();
            if (result != NightConsumerBridge.Result.IDLE) return result;
        }
        fail("Kafka failed to deliver within bounded retry window");
        return NightConsumerBridge.Result.IDLE;
    }

    @Test void beforeLeaderFailurePersistsReplicaAndCommittedOffset() throws Exception {
        Assumptions.assumeTrue(phase("before"));
        try (AdminClient admin = AdminClient.create(NightKafka.config(System.getenv()))) {
            admin.createTopics(List.of(
                    new NewTopic(topic(), 1, (short) 3).configs(Map.of("min.insync.replicas", "2"))))
                    .all().get(45, TimeUnit.SECONDS);
        }
        int leader = awaitReplicas(3);
        String leaderPath = System.getProperty("night.ha.leaderFile");
        if (leaderPath == null || !Path.of(leaderPath).isAbsolute())
            throw new IllegalArgumentException("night.ha.leaderFile must be absolute");
        Files.writeString(Path.of(leaderPath), Integer.toString(leader));
        try (NightJdbc jdbc = database(); NightKafka producer = new NightKafka();
             NightKafka consumer = new NightKafka()) {
            producer.open();
            for (int i = 1; i <= 3; i++)
                producer.publish(topic(), event(i), "{\"number\":" + i + "}");
            consumer.subscribe(topic(), group());
            var bridge = new NightConsumerBridge(new NightInbox(jdbc),
                    NightConsumerBridge.kafka(consumer, 500), namespace());
            assertEquals(NightConsumerBridge.Result.STORED, await(bridge));
            assertEquals(List.of(event(1)), storedIds(jdbc));
            // Leave the next offset uncommitted as the leader is removed.
            var uncommitted = consumer.poll(500);
            assertEquals(1, uncommitted.get("present"));
            assertEquals(event(2), uncommitted.get("message_id"));
        }
    }

    @Test void remainingMajorityElectsLeaderAndKeepsInboxIntegrity() throws Exception {
        Assumptions.assumeTrue(phase("after"));
        // The CI workflow stops the actual partition leader between JVMs.
        assertNotEquals(Integer.parseInt(Files.readString(
                Path.of(System.getProperty("night.ha.leaderFile"))).strip()), awaitReplicas(2));
        try (NightJdbc jdbc = database(); NightKafka consumer = new NightKafka();
             NightKafka producer = new NightKafka()) {
            assertEquals(List.of(event(1)), storedIds(jdbc));
            consumer.subscribe(topic(), group());
            producer.open();
            producer.publish(topic(), event(4), "{\"number\":4}");
            var bridge = new NightConsumerBridge(new NightInbox(jdbc),
                    NightConsumerBridge.kafka(consumer, 500), namespace());
            for (int i = 2; i <= 4; i++)
                assertEquals(NightConsumerBridge.Result.STORED, await(bridge));
            assertEquals(List.of(event(1), event(2), event(3), event(4)), storedIds(jdbc));
        }
        try (NightKafka restarted = new NightKafka()) {
            restarted.subscribe(topic(), group());
            for (int i = 0; i < 3; i++)
                assertEquals(0, restarted.poll(500).get("present"));
        }
    }

    @Test void returnedReplicaCatchesUpAndGroupRemainsCommitted() throws Exception {
        Assumptions.assumeTrue(phase("return"));
        awaitReplicas(3);
        try (NightJdbc jdbc = database(); NightKafka producer = new NightKafka();
             NightKafka consumer = new NightKafka()) {
            producer.open();
            producer.publish(topic(), event(5), "{\"number\":5}");
            consumer.subscribe(topic(), group());
            var bridge = new NightConsumerBridge(new NightInbox(jdbc),
                    NightConsumerBridge.kafka(consumer, 500), namespace());
            // A network partition may cause Kafka to replay an already
            // committed offset after reconnecting. That is valid at-least-once
            // delivery: the PostgreSQL inbox must return DUPLICATE and retain
            // its payload, then still ingest the newly published event 5.
            boolean eventFiveStored = false;
            for (int i = 0; i < 12; i++) {
                var outcome = await(bridge);
                if (outcome == NightConsumerBridge.Result.STORED) {
                    eventFiveStored = true;
                    break;
                }
                assertEquals(NightConsumerBridge.Result.DUPLICATE, outcome,
                        "Unexpected result while recovering a committed offset");
            }
            assertTrue(eventFiveStored, "Event 5 was not durably stored after partition healing");
            assertEquals(List.of(event(1), event(2), event(3), event(4), event(5)), storedIds(jdbc));
        }
    }
    @Test void identifyActualCurrentLeaderForMinorityFaultInjection() throws Exception {
        Assumptions.assumeTrue(phase("identify-minority-leader"));
        int leader = awaitReplicas(3);
        String path = System.getProperty("night.ha.leaderFile");
        if (path == null || !Path.of(path).isAbsolute())
            throw new IllegalArgumentException("night.ha.leaderFile must be an absolute path");
        try (NightJdbc jdbc = database()) {
            assertEquals(List.of(event(1), event(2), event(3), event(4), event(5)), storedIds(jdbc));
        }
        Files.writeString(Path.of(path), Integer.toString(leader));
    }

    @Test void minorityCannotConfirmNewWritesOrMutateDurableInbox() throws Exception {
        Assumptions.assumeTrue(phase("minority"));
        // CI has terminated TWO of the three broker/controllers. A single
        // remaining voter must not be able to confirm an acks=all write on
        // a topic requiring two in-sync replicas.
        try (NightJdbc jdbc = database(); NightKafka producer = new NightKafka()) {
            assertEquals(List.of(event(1), event(2), event(3), event(4), event(5)),
                    storedIds(jdbc));
            producer.open();
            assertThrows(IllegalStateException.class,
                    () -> producer.publish(topic(), event(6), "{\"number\":6}"),
                    "A minority must not confirm a new Kafka write");
            assertEquals(List.of(event(1), event(2), event(3), event(4), event(5)),
                    storedIds(jdbc), "Unconfirmed Kafka data must not enter the inbox");
        }
    }

    @Test void restoredMajorityRecoversWithStableEventIds() throws Exception {
        Assumptions.assumeTrue(phase("healed"));
        awaitReplicas(3);
        try (NightJdbc jdbc = database(); NightKafka producer = new NightKafka();
             NightKafka consumer = new NightKafka()) {
            // An earlier timed-out publish might still have reached a broker,
            // so reusing the SAME event ID is deliberate. Inbox deduplication
            // is the safety boundary for ambiguous producer confirmations.
            producer.open();
            producer.publish(topic(), event(6), "{\"number\":6}");
            consumer.subscribe(topic(), group());
            var bridge = new NightConsumerBridge(new NightInbox(jdbc),
                    NightConsumerBridge.kafka(consumer, 500), namespace());
            boolean receivedNewEvent = false;
            for (int i = 0; i < 20; i++) {
                var result = await(bridge);
                if (result == NightConsumerBridge.Result.STORED) {
                    receivedNewEvent = true;
                    break;
                }
                assertEquals(NightConsumerBridge.Result.DUPLICATE, result,
                        "Only already-persisted events may be replayed");
                // The retried event 6 itself might have reached a broker
                // before its failed confirmation; it is safe to treat it as
                // recovered only when the exact durable row is present.
                var persisted = storedIds(jdbc);
                if (persisted.contains(event(6))) {
                    receivedNewEvent = true;
                    break;
                }
            }
            assertTrue(receivedNewEvent, "Restored quorum must deliver the stable event ID");
            assertEquals(List.of(event(1), event(2), event(3), event(4), event(5), event(6)),
                    storedIds(jdbc));
            var stored = jdbc.query("SELECT payload FROM night_inbox WHERE inbox_namespace=? AND event_id=?",
                    List.of(namespace(), event(6)));
            assertEquals(1, stored.size());
            assertEquals("{\"number\":6}", stored.get(0).get("payload"));
        }
    }

}
