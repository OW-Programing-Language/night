package org.owlang.night.actuator;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

class NightMetricsTest {
    @Test void exposesWellFormedPrometheusCounters() {
        NightMetrics metrics = new NightMetrics();
        metrics.observe(200, 1_500_000_000L);
        metrics.observe(403, 500_000_000L);
        String output = metrics.prometheus();
        assertTrue(output.contains("night_http_requests_total 2\\n"));
        assertTrue(output.contains("night_http_access_denied_total 1\\n"));
        assertTrue(output.contains("night_http_request_duration_seconds_total 2.0\\n"));
        assertFalse(output.contains("\\\\n"));
        assertFalse(output.contains("{"));
    }

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
