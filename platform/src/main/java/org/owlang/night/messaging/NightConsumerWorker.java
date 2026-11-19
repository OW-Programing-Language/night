package org.owlang.night.messaging;

import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import org.owlang.night.data.NightInbox;
import org.owlang.night.data.NightJdbc;

/**
 * Independently supervised broker-to-inbox worker.
 *
 * Configure the broker, namespace and subscription through environment.
 * Run behind systemd/Kubernetes; it does not host OW HTTP or evaluate
 * arbitrary OW callbacks on the Kafka/RabbitMQ consumer threads.
 */
public final class NightConsumerWorker {
    private NightConsumerWorker() {}

    static String required(Map<String, String> env, String name) {
        String raw = env.get(name);
        if (raw == null || raw.isBlank())
            throw new IllegalArgumentException(name + " is required");
        return raw;
    }

    static String broker(Map<String, String> env) {
        String configured = required(env, "NIGHT_CONSUMER_BROKER");
        if (!"kafka".equals(configured) && !"rabbitmq".equals(configured))
            throw new IllegalArgumentException("NIGHT_CONSUMER_BROKER must be kafka or rabbitmq");
        return configured;
    }

    static int idleMillis(Map<String, String> env) {
        return NightOutboxWorker.integerSetting(env, "NIGHT_CONSUMER_IDLE_MS", 250, 100, 5000);
    }

    public static void main(String[] args) throws InterruptedException {
        if (args.length != 0)
            throw new IllegalArgumentException("NightConsumerWorker takes no command-line arguments");
        Map<String, String> env = System.getenv();
        String broker = broker(env);
        String namespace = NightInbox.namespace(required(env, "NIGHT_CONSUMER_NAMESPACE"));
        int idleMs = idleMillis(env);
        // Eagerly parse and validate all configured rules before connecting
        // to any broker, so configuration errors never ACK messages.
        NightMessageSchemas schemas = NightMessageSchemas.fromEnvironment(env);
        String kafkaTopic = null, kafkaGroup = null, rabbitQueue = null;
        if ("kafka".equals(broker)) {
            kafkaTopic = NightKafka.identifier(required(env, "NIGHT_CONSUMER_TOPIC"));
            kafkaGroup = NightKafka.identifier(required(env, "NIGHT_CONSUMER_GROUP"));
        } else {
            rabbitQueue = NightInbox.topic(env.getOrDefault("NIGHT_RABBITMQ_QUEUE", "night_events"));
        }

        // A DLQ is opt-in and must never be the same source topic/queue.
        // Resolve the destination before opening the database or broker.
        String dlqDestination = NightPoisonDlq.configuredDestination(env, broker,
                "kafka".equals(broker) ? kafkaTopic : rabbitQueue);

        AtomicBoolean running = new AtomicBoolean(true);
        Thread workerThread = Thread.currentThread();
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            running.set(false);
            try { workerThread.join(30_000); }
            catch (InterruptedException ex) { Thread.currentThread().interrupt(); }
        }, "night-consumer-shutdown"));

        // Migrations must already have run. This worker intentionally does not
        // migrate an active application's database at startup.
        try (NightJdbc jdbc = new NightJdbc();
             NightKafka kafka = new NightKafka();
             NightRabbitMQ rabbit = new NightRabbitMQ();
             NightKafka kafkaDlq = new NightKafka();
             NightRabbitMQ rabbitDlq = new NightRabbitMQ(
                     "rabbitmq".equals(broker) ? dlqDestination : null)) {
            jdbc.open();
            NightConsumerBridge.Source source;
            if ("kafka".equals(broker)) {
                kafka.subscribe(kafkaTopic, kafkaGroup);
                source = NightConsumerBridge.kafka(kafka, 500);
            } else {
                rabbit.open();
                source = NightConsumerBridge.rabbitMq(rabbit, rabbitQueue);
            }
            NightPoisonDlq.Publisher poisonPublisher = null;
            if (dlqDestination != null) {
                if ("kafka".equals(broker)) {
                    kafkaDlq.open();
                    poisonPublisher = NightPoisonDlq.kafka(kafkaDlq, dlqDestination);
                } else {
                    rabbitDlq.open();
                    poisonPublisher = NightPoisonDlq.rabbitMq(rabbitDlq, dlqDestination);
                }
            }
            NightConsumerBridge bridge = new NightConsumerBridge(
                    new NightInbox(jdbc), new NightBrokerQuarantine(jdbc),
                    source, namespace, schemas, poisonPublisher);
            System.out.println("Night consumer worker started (transport=" + broker
                    + ", namespace=" + namespace + ")");
            while (running.get() && !Thread.currentThread().isInterrupted()) {
                NightConsumerBridge.Result result = bridge.consumeOne();
                if (result == NightConsumerBridge.Result.IDLE)
                    Thread.sleep(idleMs);
                // Intentional fail-fast: if DB or broker is unavailable,
                // do not ACK, and let the process supervisor restart us.
            }
        }
    }
}
