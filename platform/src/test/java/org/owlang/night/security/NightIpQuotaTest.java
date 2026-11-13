package org.owlang.night.security;

import org.junit.jupiter.api.Test;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneId;
import static org.junit.jupiter.api.Assertions.*;

class NightIpQuotaTest {
    static final class MutableClock extends Clock {
        long millis;
        MutableClock(long start) { millis = start; }
        @Override public ZoneId getZone() { return ZoneId.of("UTC"); }
        @Override public Clock withZone(ZoneId zone) { return this; }
        @Override public Instant instant() { return Instant.ofEpochMilli(millis); }
        @Override public long millis() { return millis; }
    }

    @Test void isolatesPeersAndResetsWindow() {
        MutableClock clock = new MutableClock(100_000);
        NightIpQuota quota = new NightIpQuota(2, 60, 2, clock);
        assertTrue(quota.allow("192.0.2.1"));
        assertTrue(quota.allow("192.0.2.1"));
        assertFalse(quota.allow("192.0.2.1"));
        assertTrue(quota.allow("192.0.2.2"));
        clock.millis += 60_000;
        assertTrue(quota.allow("192.0.2.1"));
        assertTrue(quota.allow("192.0.2.3"));
        assertFalse(quota.allow(null));
    }

    @Test void refusesNewPeersWhenMemoryBoundReached() {
        MutableClock clock = new MutableClock(100_000);
        NightIpQuota quota = new NightIpQuota(10, 60, 1, clock);
        assertTrue(quota.allow("one"));
        assertFalse(quota.allow("two"));
        assertTrue(quota.allow("one"));
        clock.millis += 60_000;
        assertTrue(quota.allow("two"));
    }

    @Test void rejectsInvalidConfiguration() {
        assertThrows(IllegalArgumentException.class, () -> new NightIpQuota(0, 60, 5, Clock.systemUTC()));
        assertThrows(IllegalArgumentException.class, () -> new NightIpQuota(1, 0, 5, Clock.systemUTC()));
        assertThrows(IllegalArgumentException.class, () -> new NightIpQuota(1, 60, 0, Clock.systemUTC()));
    }
}
