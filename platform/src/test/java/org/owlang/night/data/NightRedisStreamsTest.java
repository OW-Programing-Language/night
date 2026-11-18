package org.owlang.night.data;

import dev.oreo.Interpreter;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Test;
import java.util.List;
import java.util.UUID;
import static org.junit.jupiter.api.Assertions.*;

class NightRedisStreamsTest {
    private static String unique() {
        return "stream_" + UUID.randomUUID().toString().replace("-", "");
    }

    @Test void rejectsUntrustedNamesUnsafeStreamIdsAndLargeBatchesWithoutConnecting() {
        assertEquals("orders", NightRedisStreams.name("orders"));
        for (String invalid : List.of("", "bad:stream", "../x", "*", "1group", "a b")) {
            assertThrows(IllegalArgumentException.class, () -> NightRedisStreams.name(invalid));
        }
        assertEquals("123-456", NightRedisStreams.entryId("123-456").toString());
        for (String invalid : List.of("*", "$", ">", "-1-0", "1", "1-2-3", "01-0",
                "9223372036854775808-0")) {
            assertThrows(IllegalArgumentException.class, () -> NightRedisStreams.entryId(invalid));
        }
        var streams = new NightRedisStreams(new NightRedis());
        assertThrows(IllegalArgumentException.class,
                () -> streams.publish("unsafe name", "event-12345678", "{}"));
        assertThrows(IllegalArgumentException.class,
                () -> streams.poll("unsafe name", "group", "worker", 1));
    }

    @Test void publishConsumeAndAcknowledgeAgainstRealRedis() {
        Assumptions.assumeTrue(System.getenv("NIGHT_REDIS_URL") != null);
        try (NightRedis redis = new NightRedis()) {
            assertEquals(1, redis.open());
            var streams = new NightRedisStreams(redis);
            String stream = unique();
            assertEquals(1, streams.createGroup(stream, "workers"));
            assertEquals(1, streams.createGroup(stream, "workers")); // no cursor reset
            assertThrows(IllegalArgumentException.class,
                    () -> streams.publish(stream, "bad", "{}"));
            assertThrows(IllegalArgumentException.class,
                    () -> streams.publish(stream, "event-12345678", "x".repeat(262_145)));
            assertThrows(IllegalArgumentException.class,
                    () -> streams.poll(stream, "workers", "one", 33));

            String id = streams.publish(stream, "event-12345678", "{\"task\":1}");
            var polled = streams.poll(stream, "workers", "one", 10);
            assertEquals(1, polled.get("present"));
            var events = (List<?>) polled.get("events");
            assertEquals(1, events.size());
            var first = (Interpreter.OwRecord) events.get(0);
            assertEquals(id, first.get("stream_id"));
            assertEquals("event-12345678", first.get("message_id"));
            assertEquals("{\"task\":1}", first.get("body"));
            assertEquals(0, streams.poll(stream, "workers", "other", 10).get("present"));
            assertEquals(1, streams.acknowledge(stream, "workers", id));
            assertEquals(0, streams.acknowledge(stream, "workers", id));
            assertEquals(0, streams.poll(stream, "workers", "one", 1).get("present"));
        }
    }

    @Test void pendingEventsCanBeRecoveredAfterWorkerDisconnect() throws InterruptedException {
        Assumptions.assumeTrue(System.getenv("NIGHT_REDIS_URL") != null);
        String stream = unique();
        String id;
        try (NightRedis firstConnection = new NightRedis()) {
            firstConnection.open();
            var streams = new NightRedisStreams(firstConnection);
            streams.createGroup(stream, "recovery");
            id = streams.publish(stream, "event-87654321", "{\"retry\":true}");
            assertEquals(1, streams.poll(stream, "recovery", "failed", 1).get("present"));
            // No ACK. Simulate the worker/connection closing unexpectedly.
        }

        Thread.sleep(1200);
        try (NightRedis recoveredConnection = new NightRedis()) {
            recoveredConnection.open();
            var streams = new NightRedisStreams(recoveredConnection);
            assertThrows(IllegalArgumentException.class,
                    () -> streams.reclaim(stream, "recovery", "healthy", 0, "0-0", 1));
            var recovered = streams.reclaim(stream, "recovery", "healthy", 1000, "0-0", 8);
            assertEquals(1, recovered.get("present"));
            assertEquals("0-0", recovered.get("next_cursor"));
            var event = (Interpreter.OwRecord) ((List<?>) recovered.get("events")).get(0);
            assertEquals(id, event.get("stream_id"));
            assertEquals(1, streams.acknowledge(stream, "recovery", id));
            assertEquals(0, streams.reclaim(stream, "recovery", "healthy", 1000, "0-0", 8).get("present"));
        }
    }
}
