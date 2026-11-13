package org.owlang.night.actuator;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

class NightMetricsTest {
    @Test void countsOnlyLowCardinalityStatuses() {
        NightMetrics metrics = new NightMetrics();
        metrics.observe(200, 10_000_000L);
        metrics.observe(401, 2_000_000L);
        metrics.observe(500, 6_000_000L);
        assertEquals(3L, metrics.snapshot().get("requests"));
        assertEquals(1L, metrics.snapshot().get("server_errors"));
        assertEquals(1L, metrics.snapshot().get("access_denied"));
        assertEquals(18L, metrics.snapshot().get("duration_ms_total"));
    }
}
