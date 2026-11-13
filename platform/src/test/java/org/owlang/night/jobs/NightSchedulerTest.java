package org.owlang.night.jobs;

import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Test;
import org.owlang.night.data.NightJdbc;

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.List;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

class NightSchedulerTest {
    static final class MutableClock extends Clock {
        Instant now = Instant.parse("2026-11-13T15:00:00Z");
        @Override public ZoneId getZone() { return ZoneOffset.UTC; }
        @Override public Clock withZone(ZoneId zone) { return this; }
        @Override public Instant instant() { return now; }
        void advanceSeconds(int seconds) { now = now.plusSeconds(seconds); }
    }

    @Test void leasesRecurringJobsAndReplaysDeadOnPostgres() {
        Assumptions.assumeTrue(System.getenv().getOrDefault("NIGHT_JDBC_URL", "").startsWith("jdbc:postgresql:"));
        MutableClock clock = new MutableClock();
        try (NightJdbc jdbc = new NightJdbc()) {
            jdbc.open();
            Path migrations = Path.of("examples/production/db/migration");
            if (!Files.isDirectory(migrations))
                migrations = Path.of("../examples/production/db/migration");
            assertTrue(Files.isDirectory(migrations));
            jdbc.migrate("filesystem:" + migrations.toAbsolutePath());

            NightScheduler scheduler = new NightScheduler(jdbc, clock);
            String once = "job-" + UUID.randomUUID();
            assertEquals(1, scheduler.schedule(once, "invoice.generate", "{}", clock.millis() - 1000, 0));
            assertEquals(0, scheduler.schedule(once, "invoice.generate", "{}", clock.millis() - 1000, 0));
            assertEquals(1, scheduler.claim("worker-a", 30).size());
            assertEquals(0, scheduler.acknowledge(once, "worker-b"));
            assertEquals(1, scheduler.acknowledge(once, "worker-a"));
            assertTrue(scheduler.claim("worker-a", 30).isEmpty());

            String repeat = "job-" + UUID.randomUUID();
            assertEquals(1, scheduler.schedule(repeat, "cleanup.daily", "{}", clock.millis() - 1000, 30));
            var first = scheduler.claim("worker-b", 30);
            assertEquals(repeat, first.get(0).get("job_id"));
            assertEquals(1, scheduler.acknowledge(repeat, "worker-b"));
            assertTrue(scheduler.claim("worker-b", 30).isEmpty());
            clock.advanceSeconds(31);
            var again = scheduler.claim("worker-b", 30);
            assertEquals(repeat, again.get(0).get("job_id"));
            assertEquals(1, scheduler.fail(repeat, "worker-b", 1, 1));
            assertTrue(scheduler.claim("worker-c", 30).isEmpty());
            assertEquals(1, scheduler.replayDead(repeat));
            var recovery = scheduler.claim("worker-c", 30);
            assertEquals(repeat, recovery.get(0).get("job_id"));
            assertEquals(1, scheduler.acknowledge(repeat, "worker-c"));
            assertEquals(0, scheduler.replayDead(repeat));
        }
    }

    @Test void validatesNamesAndDurationsBeforeSQL() {
        NightScheduler scheduler = new NightScheduler(null);
        assertThrows(IllegalArgumentException.class,
                () -> scheduler.schedule("valid-001", "echo;rm", "{}", 1L, 0));
        assertThrows(IllegalArgumentException.class,
                () -> scheduler.schedule("valid-001", "allowed.job", "{}", 1L, -1));
        assertThrows(IllegalArgumentException.class, () -> scheduler.claim("worker", 0));
        assertThrows(IllegalArgumentException.class, () -> scheduler.fail("job", "worker", 0, 5));
    }
}
