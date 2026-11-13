package org.owlang.night.data;

import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Test;
import java.util.List;
import java.util.UUID;
import static org.junit.jupiter.api.Assertions.*;

class NightOutboxTest {
    @Test void leasesRetriesAndDeadLettersOnPostgreSQL() {
        Assumptions.assumeTrue(System.getenv().getOrDefault("NIGHT_JDBC_URL", "").startsWith("jdbc:postgresql:"));
        try (NightJdbc jdbc = new NightJdbc()) {
            jdbc.open();
            jdbc.migrate("filesystem:examples/production/db/migration");
            NightOutbox outbox = new NightOutbox(jdbc);
            String id = "outbox-ci-" + UUID.randomUUID();
            jdbc.begin();
            assertEquals(1, outbox.enqueue(id, "test.event", "{\"ok\":true}"));
            jdbc.commit();
            assertEquals(0, outbox.enqueue(id, "test.event", "{\"ok\":true}"));
            assertThrows(IllegalArgumentException.class, () -> outbox.claim("worker-a", 0));
            var claimed = outbox.claim("worker-a", 30);
            assertEquals(1, claimed.size());
            assertEquals(id, claimed.get(0).get("event_id"));
            assertEquals(0, outbox.acknowledge(id, "other-worker"));
            assertEquals(1, outbox.fail(id, "worker-a", 1, 1));
            assertTrue(outbox.claim("worker-b", 30).isEmpty());
            var dead = jdbc.query("SELECT state FROM night_outbox WHERE event_id = ?", List.of(id));
            assertEquals("DEAD", dead.get(0).get("state"));
        }
    }
}
