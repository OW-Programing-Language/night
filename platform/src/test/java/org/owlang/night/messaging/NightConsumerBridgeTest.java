package org.owlang.night.messaging;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Test;
import org.owlang.night.data.NightInbox;
import org.owlang.night.data.NightJdbc;
import static org.junit.jupiter.api.Assertions.*;

class NightConsumerBridgeTest {
    private static boolean postgres() {
        return System.getenv().getOrDefault("NIGHT_JDBC_URL", "").startsWith("jdbc:postgresql:");
    }

    private static String unique(String prefix) {
        return prefix + UUID.randomUUID().toString().replace("-", "");
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

    private static int rows(NightJdbc jdbc, String namespace, String topic, String id) {
        return jdbc.query("SELECT event_id FROM night_inbox WHERE inbox_namespace=? AND topic=? AND event_id=?",
                List.of(namespace, topic, id)).size();
    }

    @Test void validatesWorkerConfigurationWithoutExternalConnections() {
        assertThrows(IllegalArgumentException.class,
                () -> NightConsumerWorker.broker(java.util.Map.of()));
        assertThrows(IllegalArgumentException.class,
                () -> NightConsumerWorker.broker(java.util.Map.of("NIGHT_CONSUMER_BROKER", "all")));
        assertEquals("kafka", NightConsumerWorker.broker(
                java.util.Map.of("NIGHT_CONSUMER_BROKER", "kafka")));
        assertEquals("rabbitmq", NightConsumerWorker.broker(
                java.util.Map.of("NIGHT_CONSUMER_BROKER", "rabbitmq")));
        assertEquals(250, NightConsumerWorker.idleMillis(java.util.Map.of()));
        assertThrows(IllegalArgumentException.class,
                () -> NightConsumerWorker.idleMillis(java.util.Map.of("NIGHT_CONSUMER_IDLE_MS", "0")));
        assertThrows(IllegalArgumentException.class,
                () -> NightConsumerBridge.kafka(new NightKafka(), 2500));
        assertThrows(IllegalArgumentException.class,
                () -> NightConsumerBridge.rabbitMq(new NightRabbitMQ(), "bad/queue"));
    }

    @Test void failedDatabaseInsertCannotAcknowledgeBroker() {
        AtomicInteger acknowledgements = new AtomicInteger();
        var source = (NightConsumerBridge.Source) () ->
                new NightConsumerBridge.Message("orders.created", "event-12345678", "{}",
                        acknowledgements::incrementAndGet);
        try (NightJdbc jdbc = new NightJdbc()) {
            // No JDBC connection: persistence must fail before broker ACK.
            var bridge = new NightConsumerBridge(new NightInbox(jdbc), source, "testgroup");
            assertThrows(IllegalStateException.class, bridge::consumeOne);
            assertEquals(0, acknowledgements.get());
        }
    }

    @Test void ackFailureAfterDatabaseCommitRedeliversAndDeduplicates() {
        Assumptions.assumeTrue(postgres());
        try (NightJdbc jdbc = database()) {
            String ns = unique("dedup_");
            String id = unique("event-");
            AtomicInteger ack = new AtomicInteger();
            var failedSource = (NightConsumerBridge.Source) () ->
                    new NightConsumerBridge.Message("orders.created", id, "{\"x\":1}",
                            () -> { ack.incrementAndGet(); throw new IllegalStateException("broker down"); });
            var first = new NightConsumerBridge(new NightInbox(jdbc), failedSource, ns);
            assertThrows(IllegalStateException.class, first::consumeOne);
            assertEquals(1, rows(jdbc, ns, "orders.created", id));

            var redelivered = (NightConsumerBridge.Source) () ->
                    new NightConsumerBridge.Message("orders.created", id, "{\"changed\":true}",
                            ack::incrementAndGet);
            var second = new NightConsumerBridge(new NightInbox(jdbc), redelivered, ns);
            assertEquals(NightConsumerBridge.Result.DUPLICATE, second.consumeOne());
            assertEquals(2, ack.get());
            assertEquals(1, rows(jdbc, ns, "orders.created", id));
            assertEquals("{\"x\":1}", jdbc.query(
                    "SELECT payload FROM night_inbox WHERE inbox_namespace=? AND topic=? AND event_id=?",
                    List.of(ns, "orders.created", id)).get(0).get("payload"));
            assertEquals(NightConsumerBridge.Result.IDLE, new NightConsumerBridge(
                    new NightInbox(jdbc), () -> null, ns).consumeOne());
        }
    }

    @Test void realRabbitMqIntakeAcknowledgesOnlyAfterDurableInsert() throws Exception {
        Assumptions.assumeTrue(postgres() && System.getenv("NIGHT_RABBITMQ_URL") != null);
        String queue = unique("night_inbox_queue_");
        try (NightJdbc jdbc = database(); NightRabbitMQ rabbit = new NightRabbitMQ(queue)) {
            rabbit.open();
            String ns = unique("rabbit_");
            String id = unique("event-");
            rabbit.publish(id, "{\"broker\":\"rabbit\"}");
            var bridge = new NightConsumerBridge(new NightInbox(jdbc),
                    NightConsumerBridge.rabbitMq(rabbit, queue), ns);
            NightConsumerBridge.Result result = NightConsumerBridge.Result.IDLE;
            for (int i = 0; i < 30 && result == NightConsumerBridge.Result.IDLE; i++) {
                result = bridge.consumeOne();
                if (result == NightConsumerBridge.Result.IDLE) Thread.sleep(50);
            }
            assertEquals(NightConsumerBridge.Result.STORED, result);
            assertEquals(1, rows(jdbc, ns, queue, id));
            rabbit.publish(id, "{\"broker\":\"rabbit\",\"duplicate\":true}");
            assertEquals(NightConsumerBridge.Result.DUPLICATE, bridge.consumeOne());
            assertEquals(1, rows(jdbc, ns, queue, id));
        }
    }

    @Test void realKafkaGroupCommitsAfterDurableInboxInsert() throws Exception {
        Assumptions.assumeTrue(postgres() && System.getenv("NIGHT_KAFKA_BOOTSTRAP_SERVERS") != null);
        String topic = unique("night_inbox_");
        String group = unique("night_workers_");
        try (AdminClient admin = AdminClient.create(NightKafka.config(System.getenv()))) {
            admin.createTopics(List.of(new NewTopic(topic, 1, (short) 1)))
                    .all().get(30, TimeUnit.SECONDS);
        }
        String ns = unique("kafka_");
        String id = unique("event-");
        try (NightJdbc jdbc = database(); NightKafka producer = new NightKafka();
             NightKafka consumer = new NightKafka()) {
            producer.open();
            producer.publish(topic, id, "{\"broker\":\"kafka\"}");
            consumer.subscribe(topic, group);
            var bridge = new NightConsumerBridge(new NightInbox(jdbc),
                    NightConsumerBridge.kafka(consumer, 500), ns);
            NightConsumerBridge.Result result = NightConsumerBridge.Result.IDLE;
            for (int i = 0; i < 30 && result == NightConsumerBridge.Result.IDLE; i++)
                result = bridge.consumeOne();
            assertEquals(NightConsumerBridge.Result.STORED, result);
            assertEquals(1, rows(jdbc, ns, topic, id));
        }
        // A separate consumer in the same group must not replay the committed offset.
        try (NightKafka restart = new NightKafka(); NightJdbc jdbc = database()) {
            restart.subscribe(topic, group);
            var bridge = new NightConsumerBridge(new NightInbox(jdbc),
                    NightConsumerBridge.kafka(restart, 500), ns);
            for (int i = 0; i < 4; i++)
                assertEquals(NightConsumerBridge.Result.IDLE, bridge.consumeOne());
            assertEquals(1, rows(jdbc, ns, topic, id));
        }
    }
}
