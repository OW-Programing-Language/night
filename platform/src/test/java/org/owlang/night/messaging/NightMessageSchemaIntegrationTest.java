package org.owlang.night.messaging;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayDeque;
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

class NightMessageSchemaIntegrationTest {
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

    private static NightMessageSchemas forTopic(String topic) {
        return NightMessageSchemas.parse("""
                {"version":1,"topics":{"%s":[
                    ["orderId","integer",1,1000],
                    ["shipping","object",1,1,[["city","string",1,40]]]
                ]}}
                """.formatted(topic));
    }

    private static int count(NightJdbc jdbc, String table, String namespace) {
        return jdbc.query("SELECT inbox_namespace FROM " + table
                + " WHERE inbox_namespace = ?", List.of(namespace)).size();
    }

    @Test void validatesBeforeAckAndNeverTreatsUnconfiguredTopicsAsValid() {
        Assumptions.assumeTrue(postgres());
        try (NightJdbc jdbc = database()) {
            String ns = unique("schema_failure_");
            AtomicInteger acks = new AtomicInteger();
            var pending = new ArrayDeque<NightConsumerBridge.Message>();
            pending.add(new NightConsumerBridge.Message("orders.created",
                    "event-12345678", "{\"orderId\":\"wrong\"}", "kafka",
                    "orders.created:0:123", acks::incrementAndGet));
            pending.add(new NightConsumerBridge.Message("orders.created",
                    "event-87654321", "{\"orderId\":9,\"shipping\":{\"city\":\"Paris\"}}",
                    "kafka", "orders.created:0:124", acks::incrementAndGet));
            var source = (NightConsumerBridge.Source) pending::poll;
            NightMessageSchemas schemas = forTopic("orders.created");
            assertThrows(IllegalArgumentException.class, () ->
                    new NightConsumerBridge(new NightInbox(jdbc),
                            null, source, ns, schemas));
            var bridge = new NightConsumerBridge(new NightInbox(jdbc),
                    new NightBrokerQuarantine(jdbc), source, ns, schemas);
            assertEquals(NightConsumerBridge.Result.QUARANTINED, bridge.consumeOne());
            assertEquals(1, acks.get());
            assertEquals(1, count(jdbc, "night_broker_quarantine", ns));
            assertEquals("SCHEMA_VIOLATION",
                    jdbc.query("SELECT reason_code FROM night_broker_quarantine WHERE inbox_namespace = ?",
                            List.of(ns)).get(0).get("reason_code"));
            assertEquals(0, count(jdbc, "night_inbox", ns));
            assertEquals(NightConsumerBridge.Result.STORED, bridge.consumeOne());
            assertEquals(2, acks.get());
            assertEquals(1, count(jdbc, "night_inbox", ns));

            var unknown = new NightConsumerBridge(new NightInbox(jdbc),
                    new NightBrokerQuarantine(jdbc),
                    () -> new NightConsumerBridge.Message("unconfigured", "event-11111111",
                            "{}", "kafka", "unconfigured:0:1", acks::incrementAndGet),
                    ns, schemas);
            assertThrows(IllegalStateException.class, unknown::consumeOne);
            assertEquals(2, acks.get());
        }
    }

    @Test void realRabbitMqQuarantinesSchemaViolationsBeforeAck() throws Exception {
        Assumptions.assumeTrue(postgres() && System.getenv("NIGHT_RABBITMQ_URL") != null);
        String queue = unique("night_schema_queue_");
        String ns = unique("rabbit_schema_");
        try (NightJdbc jdbc = database(); NightRabbitMQ rabbit = new NightRabbitMQ(queue)) {
            rabbit.open();
            String bad = unique("event-");
            String good = unique("event-");
            rabbit.publish(bad, "{\"orderId\":\"not-a-number\"}");
            rabbit.publish(good, "{\"orderId\":12,\"shipping\":{\"city\":\"Lyon\"}}");
            var bridge = new NightConsumerBridge(new NightInbox(jdbc),
                    new NightBrokerQuarantine(jdbc),
                    NightConsumerBridge.rabbitMq(rabbit, queue), ns, forTopic(queue));
            NightConsumerBridge.Result first = NightConsumerBridge.Result.IDLE;
            for (int i = 0; i < 30 && first == NightConsumerBridge.Result.IDLE; i++) {
                first = bridge.consumeOne();
                if (first == NightConsumerBridge.Result.IDLE) Thread.sleep(50);
            }
            assertEquals(NightConsumerBridge.Result.QUARANTINED, first);
            assertEquals(NightConsumerBridge.Result.STORED, bridge.consumeOne());
            assertEquals(1, count(jdbc, "night_broker_quarantine", ns));
            assertEquals(1, count(jdbc, "night_inbox", ns));
            assertEquals(good, jdbc.query(
                    "SELECT event_id FROM night_inbox WHERE inbox_namespace = ?", List.of(ns))
                    .get(0).get("event_id"));
            assertEquals("SCHEMA_VIOLATION", jdbc.query(
                    "SELECT reason_code FROM night_broker_quarantine WHERE inbox_namespace = ?",
                    List.of(ns)).get(0).get("reason_code"));
        }
    }

    @Test void realKafkaQuarantinesBadJsonAndCommitsBeforeValidEvent() throws Exception {
        Assumptions.assumeTrue(postgres() && System.getenv("NIGHT_KAFKA_BOOTSTRAP_SERVERS") != null);
        String topic = unique("night_schema_");
        String group = unique("night_schema_group_");
        String ns = unique("kafka_schema_");
        try (AdminClient admin = AdminClient.create(NightKafka.config(System.getenv()))) {
            admin.createTopics(List.of(new NewTopic(topic, 1, (short) 1)))
                    .all().get(30, TimeUnit.SECONDS);
        }
        try (NightJdbc jdbc = database(); NightKafka producer = new NightKafka();
             NightKafka consumer = new NightKafka()) {
            producer.open();
            producer.publish(topic, unique("event-"), "{");
            String good = unique("event-");
            producer.publish(topic, good, "{\"orderId\":13,\"shipping\":{\"city\":\"Nice\"}}");
            consumer.subscribe(topic, group);
            var bridge = new NightConsumerBridge(new NightInbox(jdbc),
                    new NightBrokerQuarantine(jdbc),
                    NightConsumerBridge.kafka(consumer, 500), ns, forTopic(topic));
            NightConsumerBridge.Result first = NightConsumerBridge.Result.IDLE;
            for (int i = 0; i < 30 && first == NightConsumerBridge.Result.IDLE; i++)
                first = bridge.consumeOne();
            assertEquals(NightConsumerBridge.Result.QUARANTINED, first);
            assertEquals("INVALID_JSON", jdbc.query(
                    "SELECT reason_code FROM night_broker_quarantine WHERE inbox_namespace = ?",
                    List.of(ns)).get(0).get("reason_code"));

            NightConsumerBridge.Result second = NightConsumerBridge.Result.IDLE;
            for (int i = 0; i < 30 && second == NightConsumerBridge.Result.IDLE; i++)
                second = bridge.consumeOne();
            assertEquals(NightConsumerBridge.Result.STORED, second);
            assertEquals(good, jdbc.query(
                    "SELECT event_id FROM night_inbox WHERE inbox_namespace = ?", List.of(ns))
                    .get(0).get("event_id"));
        }
        try (NightKafka restarted = new NightKafka(); NightJdbc jdbc = database()) {
            restarted.subscribe(topic, group);
            var bridge = new NightConsumerBridge(new NightInbox(jdbc),
                    new NightBrokerQuarantine(jdbc),
                    NightConsumerBridge.kafka(restarted, 500), ns, forTopic(topic));
            for (int i = 0; i < 3; i++)
                assertEquals(NightConsumerBridge.Result.IDLE, bridge.consumeOne());
            assertEquals(1, count(jdbc, "night_inbox", ns));
        }
    }
}
