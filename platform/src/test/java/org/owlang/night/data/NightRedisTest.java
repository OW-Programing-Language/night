package org.owlang.night.data;

import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Test;
import java.util.UUID;
import static org.junit.jupiter.api.Assertions.*;

class NightRedisTest {
    @Test void requiresTlsExceptExplicitLocalIntegration() {
        assertThrows(IllegalArgumentException.class,
                () -> NightRedis.validateUrl("redis://redis.internal:6379", false));
        assertThrows(IllegalArgumentException.class,
                () -> NightRedis.validateUrl("redis://redis.internal:6379", true));
        assertThrows(IllegalArgumentException.class,
                () -> NightRedis.validateUrl("redis://127.0.0.1:6379", false));
        assertDoesNotThrow(() -> NightRedis.validateUrl("redis://127.0.0.1:6379", true));
        assertDoesNotThrow(() -> NightRedis.validateUrl("rediss://redis.example.com:6380", false));
        assertThrows(IllegalArgumentException.class,
                () -> NightRedis.validateUrl("http://redis.example.com:6380", false));
    }

    @Test void namespacedTtlCacheAndInvalidation() {
        Assumptions.assumeTrue(System.getenv("NIGHT_REDIS_URL") != null);
        try (NightRedis redis = new NightRedis()) {
            assertEquals(1, redis.open());
            assertEquals(1, redis.health());
            String key = "ci:" + UUID.randomUUID().toString().replace("-", "");
            assertEquals("", redis.get(key));
            assertEquals(1, redis.put(key, "hello", 30));
            assertEquals("hello", redis.get(key));
            assertEquals(1, redis.evict(key));
            assertEquals("", redis.get(key));
            assertThrows(IllegalArgumentException.class, () -> redis.put("*", "bad", 30));
            assertThrows(IllegalArgumentException.class, () -> redis.put(key, "bad", 0));
        }
    }
}
