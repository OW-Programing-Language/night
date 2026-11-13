package org.owlang.night.messaging;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.GetResponse;
import dev.oreo.Interpreter;

import java.io.IOException;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.TimeoutException;

/**
 * RabbitMQ durable queue adapter with publisher confirms and manual ACKs.
 *
 * All Channel operations are synchronized (RabbitMQ channels are not
 * generally thread safe). The public API uses short-lived polling rather
 * than arbitrary callback execution in OW. Broker handling is at-least-once.
 */
public final class NightRabbitMQ implements AutoCloseable {
    private Connection connection;
    private Channel channel;
    private String queue;

    public static void validateUri(String raw, boolean testLoopback) {
        if (raw == null || raw.isBlank()) throw new IllegalArgumentException("Missing RabbitMQ URL");
        URI uri = URI.create(raw);
        if (uri.getHost() == null || uri.getRawQuery() != null || uri.getRawFragment() != null)
            throw new IllegalArgumentException("Invalid RabbitMQ URI");
        if ("amqps".equals(uri.getScheme())) return;
        boolean loopback = "localhost".equalsIgnoreCase(uri.getHost())
                || "127.0.0.1".equals(uri.getHost())
                || "::1".equals(uri.getHost());
        if (!"amqp".equals(uri.getScheme()) || !testLoopback || !loopback)
            throw new IllegalArgumentException("RabbitMQ requires amqps:// TLS outside loopback CI");
    }

    public synchronized int open() {
        if (channel != null && channel.isOpen()) return 1;
        String uri = System.getenv("NIGHT_RABBITMQ_URL");
        validateUri(uri, "true".equals(System.getenv("NIGHT_RABBITMQ_ALLOW_INSECURE_LOOPBACK")));
        String configured = System.getenv().getOrDefault("NIGHT_RABBITMQ_QUEUE", "night_events");
        if (!configured.matches("[A-Za-z][A-Za-z0-9_.-]{0,63}"))
            throw new IllegalArgumentException("Invalid RabbitMQ queue name");
        ConnectionFactory factory = new ConnectionFactory();
        try {
            factory.setUri(uri);
            factory.setAutomaticRecoveryEnabled(true);
            factory.setNetworkRecoveryInterval(5_000);
            factory.setConnectionTimeout(5_000);
            factory.setRequestedHeartbeat(30);
            if (uri.startsWith("amqps://")) factory.enableHostnameVerification();
            connection = factory.newConnection("night-platform");
            channel = connection.createChannel();
            channel.queueDeclare(configured, true, false, false, null);
            channel.basicQos(10);
            channel.confirmSelect();
            queue = configured;
            return 1;
        } catch (Exception ex) {
            close();
            throw new IllegalStateException("Night RabbitMQ connection failed", ex);
        }
    }

    private Channel ready() {
        if (channel == null || !channel.isOpen())
            throw new IllegalStateException("Night RabbitMQ is not open");
        return channel;
    }

    public synchronized int publish(String messageId, String jsonBody) {
        if (messageId == null || !messageId.matches("[A-Za-z0-9_-]{8,80}")
                || jsonBody == null || jsonBody.length() > 262_144)
            throw new IllegalArgumentException("Invalid RabbitMQ message");
        try {
            var properties = new AMQP.BasicProperties.Builder()
                    .messageId(messageId).contentType("application/json")
                    .deliveryMode(2).build();
            // Default exchange with an already declared durable named queue.
            ready().basicPublish("", queue, true, properties, jsonBody.getBytes(StandardCharsets.UTF_8));
            ready().waitForConfirmsOrDie(5_000);
            return 1;
        } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("RabbitMQ publish interrupted", ex);
        } catch (IOException | TimeoutException ex) {
            throw new IllegalStateException("RabbitMQ publish unconfirmed", ex);
        }
    }

    /** One message at a time, manual ACK after the business side effect. */
    public synchronized Interpreter.OwRecord poll() {
        try {
            GetResponse next = ready().basicGet(queue, false);
            Interpreter.OwRecord out = new Interpreter.OwRecord();
            if (next == null) {
                out.put("present", 0);
                return out;
            }
            out.put("present", 1);
            out.put("delivery_tag", next.getEnvelope().getDeliveryTag());
            out.put("message_id", next.getProps().getMessageId() == null
                    ? "" : next.getProps().getMessageId());
            out.put("body", new String(next.getBody(), StandardCharsets.UTF_8));
            out.put("redelivered", next.getEnvelope().isRedeliver() ? 1 : 0);
            return out;
        } catch (IOException ex) {
            throw new IllegalStateException("RabbitMQ poll failed", ex);
        }
    }

    public synchronized int ack(long deliveryTag) {
        if (deliveryTag <= 0) throw new IllegalArgumentException("Invalid RabbitMQ delivery tag");
        try {
            ready().basicAck(deliveryTag, false);
            return 1;
        } catch (IOException ex) {
            throw new IllegalStateException("RabbitMQ acknowledge failed", ex);
        }
    }

    public synchronized int reject(long deliveryTag, boolean requeue) {
        if (deliveryTag <= 0) throw new IllegalArgumentException("Invalid RabbitMQ delivery tag");
        try {
            ready().basicNack(deliveryTag, false, requeue);
            return 1;
        } catch (IOException ex) {
            throw new IllegalStateException("RabbitMQ reject failed", ex);
        }
    }

    @Override public synchronized void close() {
        if (channel != null) {
            try { channel.close(); } catch (Exception ignored) { }
            channel = null;
        }
        if (connection != null) {
            try { connection.close(); } catch (Exception ignored) { }
            connection = null;
        }
    }
}
