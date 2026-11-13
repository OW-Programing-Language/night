package org.owlang.night.actuator;

import dev.oreo.Interpreter;
import java.util.concurrent.atomic.LongAdder;

/** Low-cardinality HTTP/latency counters, no raw URLs or client IDs. */
public final class NightMetrics {
    private final LongAdder total = new LongAdder();
    private final LongAdder errors = new LongAdder();
    private final LongAdder unauthorized = new LongAdder();
    private final LongAdder durationMillis = new LongAdder();

    public void observe(int status, long elapsedNanos) {
        total.increment();
        if (status >= 500) errors.increment();
        if (status == 401 || status == 403) unauthorized.increment();
        durationMillis.add(Math.max(0, elapsedNanos / 1_000_000));
    }

    /** Prometheus 0.0.4 exposition text, no user-controlled labels. */
    public String prometheus() {
        return "# HELP night_http_requests_total Total HTTP requests observed by Night\n"
             + "# TYPE night_http_requests_total counter\n"
             + "night_http_requests_total " + total.sum() + "\n"
             + "# HELP night_http_server_errors_total HTTP 5xx responses\n"
             + "# TYPE night_http_server_errors_total counter\n"
             + "night_http_server_errors_total " + errors.sum() + "\n"
             + "# HELP night_http_access_denied_total HTTP 401/403 responses\n"
             + "# TYPE night_http_access_denied_total counter\n"
             + "night_http_access_denied_total " + unauthorized.sum() + "\n"
             + "# HELP night_http_request_duration_seconds_total Aggregate request duration\n"
             + "# TYPE night_http_request_duration_seconds_total counter\n"
             + "night_http_request_duration_seconds_total " + (durationMillis.sum() / 1000.0) + "\n";
    }

    public Interpreter.OwRecord snapshot() {
        Interpreter.OwRecord result = new Interpreter.OwRecord();
        result.put("requests", total.sum());
        result.put("server_errors", errors.sum());
        result.put("access_denied", unauthorized.sum());
        result.put("duration_ms_total", durationMillis.sum());
        return result;
    }
}
