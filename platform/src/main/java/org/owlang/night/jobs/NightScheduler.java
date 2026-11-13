package org.owlang.night.jobs;

import dev.oreo.Interpreter;
import org.owlang.night.data.NightJdbc;

import java.time.Clock;
import java.util.List;

/**
 * PostgreSQL durable schedule with transactionally leased, named jobs.
 *
 * It NEVER deserializes or evaluates executable code. Applications must
 * dispatch each allowlisted task_name to their own explicit OW handlers.
 * Lease expiry means at-least-once execution, not exactly-once.
 */
public final class NightScheduler {
    private final NightJdbc jdbc;
    private final Clock clock;

    public NightScheduler(NightJdbc jdbc) {
        this(jdbc, Clock.systemUTC());
    }

    NightScheduler(NightJdbc jdbc, Clock clock) {
        this.jdbc = jdbc;
        this.clock = clock;
    }

    private static String validate(String input, String label, int max) {
        if (input == null || input.isBlank() || input.length() > max)
            throw new IllegalArgumentException("Invalid scheduler " + label);
        return input;
    }

    /** Caller supplies stable job ID to make duplicate scheduling idempotent. */
    public int schedule(String jobId, String taskName, String payload,
            long firstRunMillis, int repeatSeconds) {
        validate(jobId, "job ID", 80);
        if (!jobId.matches("[A-Za-z0-9_-]{3,80}"))
            throw new IllegalArgumentException("Invalid scheduler job ID");
        if (taskName == null || !taskName.matches("[A-Za-z][A-Za-z0-9_.-]{0,63}"))
            throw new IllegalArgumentException("Invalid scheduler task name");
        if (payload == null || payload.length() > 262_144
                || firstRunMillis < 1 || repeatSeconds < 0 || repeatSeconds > 31_536_000)
            throw new IllegalArgumentException("Invalid scheduler timing or payload");
        return jdbc.execute("""
                INSERT INTO night_scheduled_jobs
                    (job_id, task_name, payload, state, next_run_at, repeat_seconds)
                VALUES (?, ?, ?, 'READY', ?, ?)
                ON CONFLICT(job_id) DO NOTHING
                """, List.of(jobId, taskName, payload, firstRunMillis, repeatSeconds));
    }

    /** Atomically claim one due job, or return an empty result when no job is due. */
    public List<Interpreter.OwRecord> claim(String worker, int leaseSeconds) {
        validate(worker, "worker", 128);
        if (leaseSeconds < 1 || leaseSeconds > 3600)
            throw new IllegalArgumentException("Lease must be 1..3600 seconds");
        long now = clock.millis();
        return jdbc.query("""
                UPDATE night_scheduled_jobs SET state='LEASED', attempts=attempts+1,
                    lease_owner=?, lease_until=?
                WHERE job_id = (
                    SELECT job_id FROM night_scheduled_jobs
                    WHERE (state='READY' AND next_run_at <= ?)
                       OR (state='LEASED' AND lease_until < ?)
                    ORDER BY next_run_at, job_id LIMIT 1 FOR UPDATE SKIP LOCKED
                )
                RETURNING job_id, task_name, payload, attempts, repeat_seconds
                """, List.of(worker, now + leaseSeconds * 1_000L, now, now));
    }

    public int acknowledge(String jobId, String worker) {
        validate(jobId, "job ID", 80);
        validate(worker, "worker", 128);
        long now = clock.millis();
        return jdbc.execute("""
                UPDATE night_scheduled_jobs SET
                    state = CASE WHEN repeat_seconds = 0 THEN 'COMPLETED' ELSE 'READY' END,
                    next_run_at = CASE WHEN repeat_seconds = 0
                                       THEN next_run_at ELSE ? + repeat_seconds * 1000 END,
                    attempts = 0, lease_owner=NULL, lease_until=0
                WHERE job_id=? AND lease_owner=? AND state='LEASED' AND lease_until >= ?
                """, List.of(now, jobId, worker, now));
    }

    public int fail(String jobId, String worker, int retrySeconds, int maxAttempts) {
        validate(jobId, "job ID", 80);
        validate(worker, "worker", 128);
        if (retrySeconds < 1 || retrySeconds > 86_400 || maxAttempts < 1 || maxAttempts > 1_000)
            throw new IllegalArgumentException("Invalid scheduler retry policy");
        long now = clock.millis();
        return jdbc.execute("""
                UPDATE night_scheduled_jobs SET
                    state=CASE WHEN attempts >= ? THEN 'DEAD' ELSE 'READY' END,
                    next_run_at=?, lease_owner=NULL, lease_until=0
                WHERE job_id=? AND lease_owner=? AND state='LEASED' AND lease_until >= ?
                """, List.of(maxAttempts, now + retrySeconds * 1000L, jobId, worker, now));
    }

    /** Explicit admin action. Caller must restrict access to this operation. */
    public int replayDead(String jobId) {
        validate(jobId, "job ID", 80);
        return jdbc.execute("""
                UPDATE night_scheduled_jobs SET state='READY', attempts=0,
                    next_run_at=?, lease_owner=NULL, lease_until=0
                WHERE job_id=? AND state='DEAD'
                """, List.of(clock.millis(), jobId));
    }
}
