package org.owlang.night.messaging;

import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import org.owlang.night.data.NightJdbc;
import org.owlang.night.data.NightOutbox;

/**
 * Standalone JVM entrypoint for dispatching a PostgreSQL transactional outbox.
 * It deliberately runs independently from Night's single-worker HTTP runtime.
 *
 * Start after Flyway migrations are applied, in a supervised systemd/Compose
 * or Kubernetes worker deployment. One worker instance routes to one broker;
 * NEVER run both Kafka and RabbitMQ workers on the same outbox table.
 */
public final class NightOutboxWorker {
    private NightOutboxWorker() {}

    static int integerSetting(Map<String, String> env, String name,
                              int fallback, int minimum, int maximum) {
        String raw = env.get(name);
        if (raw == null || raw.isBlank()) return fallback;
        try {
            int value = Integer.parseInt(raw);
            if (value < minimum || value > maximum)
                throw new IllegalArgumentException(name + " must be " + minimum + ".." + maximum);
            return value;
        } catch (NumberFormatException ex) {
            throw new IllegalArgumentException(name + " must be an integer", ex);
        }
    }

    static String brokerSetting(Map<String, String> env) {
        String value = env.get("NIGHT_OUTBOX_BROKER");
        if (!"kafka".equals(value) && !"rabbitmq".equals(value))
            throw new IllegalArgumentException("NIGHT_OUTBOX_BROKER must be kafka or rabbitmq");
        return value;
    }

    static NightOutboxDispatcher.Policy policy(Map<String, String> env) {
        return new NightOutboxDispatcher.Policy(
                integerSetting(env, "NIGHT_OUTBOX_LEASE_SECONDS", 60, 30, 3600),
                integerSetting(env, "NIGHT_OUTBOX_MAX_ATTEMPTS", 8, 1, 1000),
                integerSetting(env, "NIGHT_OUTBOX_RETRY_SECONDS", 2, 1, 86400),
                integerSetting(env, "NIGHT_OUTBOX_MAX_RETRY_SECONDS", 300, 1, 86400));
    }

    public static void main(String[] args) throws InterruptedException {
        if (args.length != 0)
            throw new IllegalArgumentException("NightOutboxWorker takes no command-line arguments");
        Map<String, String> env = System.getenv();
        String broker = brokerSetting(env);
        NightOutboxDispatcher.Policy policy = policy(env);
        int pollMillis = integerSetting(env, "NIGHT_OUTBOX_IDLE_POLL_MS", 500, 100, 5000);
        // Use a unique lease owner per process restart to prevent stale ACK.
        String worker = "night-" + UUID.randomUUID().toString().replace("-", "");
        AtomicBoolean running = new AtomicBoolean(true);
        Thread workerThread = Thread.currentThread();
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            running.set(false);
            // Give an in-flight publish/ACK a chance to complete; otherwise
            // its lease will expire and another worker will retry.
            try {
                workerThread.join(70_000);
            } catch (InterruptedException ex) {
                Thread.currentThread().interrupt();
            }
        }, "night-outbox-shutdown"));

        // Never call Flyway migrations from the background worker; run them
        // during deployment before this service starts.
        try (NightJdbc jdbc = new NightJdbc();
             NightKafka kafka = new NightKafka();
             NightRabbitMQ rabbit = new NightRabbitMQ()) {
            jdbc.open();
            if ("kafka".equals(broker)) kafka.open();
            else rabbit.open();

            NightOutboxDispatcher.Publisher target = (id, topic, payload) -> {
                if ("kafka".equals(broker)) {
                    try {
                        kafka.open();
                        kafka.publish(topic, id, payload);
                    } catch (RuntimeException ex) {
                        kafka.close();
                        throw ex;
                    }
                } else {
                    // RabbitMQ uses its configured durable queue.
                    // The outbox topic is logical metadata, not a queue selector.
                    try {
                        rabbit.open();
                        if (rabbit.publish(id, payload) != 1)
                            throw new IllegalStateException("RabbitMQ did not confirm event delivery");
                    } catch (RuntimeException ex) {
                        rabbit.close();
                        throw ex;
                    }
                }
            };

            NightOutboxDispatcher dispatcher = new NightOutboxDispatcher(
                    new NightOutbox(jdbc), target, worker, policy);
            System.out.println("Night outbox worker started (transport=" + broker + ")");
            while (running.get() && !Thread.currentThread().isInterrupted()) {
                NightOutboxDispatcher.DispatchResult outcome = dispatcher.dispatchOne();
                switch (outcome.status()) {
                    case "DELIVERED", "RETRY", "DEAD", "LEASE_LOST" ->
                            System.out.println("Night outbox " + outcome.status()
                                    + " event=" + outcome.eventId()
                                    + " attempt=" + outcome.attempts());
                    case "IDLE" -> Thread.sleep(pollMillis);
                    default -> throw new IllegalStateException("Unexpected dispatcher status");
                }
            }
        }
    }
}
