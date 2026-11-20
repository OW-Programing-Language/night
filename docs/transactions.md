# Night JDBC transaction isolation and migration recovery

Night 0.2 supports `database.begin()` and `database.begin_isolation(level)`, where `level` is exactly `READ_COMMITTED`, `REPEATABLE_READ`, or `SERIALIZABLE`. The database and driver determine the actual guarantees. Nested transactions are unsupported. On controller completion, any uncommitted transaction is rolled back through the OW runtime extension's after-request hook.

```ow
set db: new NightData()
call: db.open()
call: db.begin_isolation("SERIALIZABLE")
# Use bound SQL parameters, then explicitly commit on success:
call: db.commit()
```

For failed operations, catch exceptions and call `db.rollback()` before propagating the error. The after-request rollback is a final safety net, not a substitute for explicit handling. `SERIALIZABLE` conflicts may require retrying the **entire transaction** with a bounded, jittered retry policy; do not automatically retry non-idempotent externally visible side effects.

## PostgreSQL validation

CI runs a separate `postgres-integration` job against a real PostgreSQL 16 service and checks isolation selection, explicit rollback, implicit request-end rollback and commit persistence. H2 checks remain in the main platform job. CI passing is not a production readiness certification.

## Schema migration rollback runbook

Flyway forward migrations are enabled; automatic `clean` is disabled. SQL schema migration rollback is **not** implemented as an automatic Night API.

1. Rehearse the migration against a restored, disposable production-like database before deployment.
2. Verify physical/PITR or logical backup and record the restore point and exact application/schema versions.
3. Prefer backward-compatible expand/contract migrations that allow reverting application binaries without reverting data.
4. Halt writer traffic when necessary; snapshot WAL/backup state and confirm database recovery privileges.
5. If a destructive migration fails, stop writes and restore from a tested backup/PITR target into a fresh database. Verify schema, migration history, counts and application smoke tests before repointing traffic.
6. Never run Flyway `clean` against production. Coordinate restore with infrastructure operators and record any potential data-loss window and recovery-point objective.

## Remaining limitations

With the experimental OW language PR #64, `@Transactional` adds automatic
JDBC begin/commit/rollback to annotated OW methods. Nested annotated calls
join the outer transaction, and caught inner failures leave the transaction
rollback-only. See [mapped transactions](mapped-transactions.md). The OW
language already supports structured `finally`. A separate high-level
transaction closure for arbitrary unannotated OW code is still not provided. PostgreSQL failover, concurrent migrations and restore drills on a deployed environment remain outstanding 1.0 gates.
