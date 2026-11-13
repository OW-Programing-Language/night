# Night durable PostgreSQL scheduler (experimental)

NightScheduler offers a persistence-backed, at-least-once scheduling primitive with one-shot or fixed-interval jobs. It uses the same PostgreSQL JDBC pool as other Night modules and a versioned Flyway migration (`V5__night_scheduled_jobs.sql`).

## Use

```ow
import "../../src/modules/jobs/NightScheduler.ow"
set scheduler: new NightScheduler()
# first_run_ms is epoch milliseconds; 0 repeat interval means one-shot.
call: scheduler.schedule("email-reminder-1001", "mail.reminder", "{}", 1794550000000, 0)
set jobs: scheduler.claim("worker-1", 30)
# Dispatch each allowlisted task_name to a trusted OW handler.
# Only after the handler's side effect succeeds:
call: scheduler.acknowledge("email-reminder-1001", "worker-1")
```

For recurring jobs, set `every_seconds` from 1 to 31,536,000. A successful acknowledgement schedules the next run relative to the current clock, so it does not replay all missed intervals after downtime. Workers may call `fail(job_id, worker, retry_seconds, max_attempts)`; exhausted failures become `DEAD`. Administrators can call `replay_dead(job_id)` after fixing the underlying issue.

Jobs are leased atomically using PostgreSQL `SELECT ... FOR UPDATE SKIP LOCKED`. Worker names and lease durations are bounded. An expired lease allows another worker to claim the same job. **At-least-once execution is mandatory**; handlers must implement idempotency using job ID and/or business keys. A late worker could still finish an external action after losing its lease.

## Safe OW callback model

Night deliberately **does not** evaluate code, script strings, class names or arbitrary callbacks from the database. The `task_name` is an opaque identifier for an explicit allowlist implemented by the application, not an execution command. Native automatic callback dispatch, request context propagation, per-tenant authorization and distributed fencing tokens remain unimplemented.

## Limitations and production gates

- No autonomous background dispatcher process (applications must poll and dispatch).
- No Quartz cron expressions, calendar/time-zone scheduling, pause/resume or misfire rules.
- No durable execution-result history beyond state/attempts, distributed fencing tokens or metrics.
- Job API requires PostgreSQL for atomic `SKIP LOCKED` leases. H2 is not a supported lease backend.
- Must test crash/lease expiry, worker duplication, concurrency and full job shutdown behavior in a real deployment.

See `NightSchedulerTest` for PostgreSQL schedule/claim/ack/retry/dead-letter replay coverage.
