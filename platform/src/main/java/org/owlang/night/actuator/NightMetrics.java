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

    public Interpreter.OwRecord snapshot() {
        Interpreter.OwRecord result = new Interpreter.OwRecord();
        result.put("requests", total.sum());
        result.put("server_errors", errors.sum());
        result.put("access_denied", unauthorized.sum());
        result.put("duration_ms_total", durationMillis.sum());
        return result;
    }
}
