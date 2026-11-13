package org.owlang.night.messaging;

import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Test;

import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

class NightRabbitMQTest {
    @Test void requiresTLSExceptExplicitLocalTesting() {
        assertThrows(IllegalArgumentException.class,
                () -> NightRabbitMQ.validateUri("amqp://mq.internal:5672", false));
        assertThrows(IllegalArgumentException.class,
                () -> NightRabbitMQ.validateUri("amqp://mq.internal:5672", true));
        assertDoesNotThrow(() -> NightRabbitMQ.validateUri("amqps://mq.example.com:5671", false));
        assertDoesNotThrow(() -> NightRabbitMQ.validateUri("amqp://127.0.0.1:5672", true));
        assertThrows(IllegalArgumentException.class,
                () -> NightRabbitMQ.validateUri("http://mq.internal", true));
    }

    @Test void publishesAndAcknowledgesDurableMessageOnRealBroker() {
        Assumptions.assumeTrue(System.getenv("NIGHT_RABBITMQ_URL") != null);
        try (NightRabbitMQ mq = new NightRabbitMQ()) {
            assertEquals(1, mq.open());
            String id = "night-" + UUID.randomUUID();
            assertEquals(1, mq.publish(id, "{\"test\":true}"));
            var received = mq.poll();
            assertEquals(1, received.get("present"));
            assertEquals(id, received.get("message_id"));
            assertEquals("{\"test\":true}", received.get("body"));
            assertEquals(1, mq.ack(((Number) received.get("delivery_tag")).longValue()));
            assertEquals(0, mq.poll().get("present"));
        }
    }
}
