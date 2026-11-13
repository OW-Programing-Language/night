# Night upgrade, compatibility and rollback policy (draft for 1.0)

This is an **operational policy proposal**, not evidence that an end-to-end upgrade has passed. Night 0.2 is experimental; no long-term support or production compatibility guarantee is claimed.

## Versioning

- Publish semver `MAJOR.MINOR.PATCH` once the public API stabilizes. Pre-1.0 changes may break APIs and must document such changes prominently.
- Pin the OW compiler/runtime commit and exact platform artifact in each tagged release. An OW runtime extension ABI change requires coordinated Night version testing.
- PATCH: compatible bug/security fixes, no mandatory schema breaking changes.
- MINOR: additive OW APIs/modules, optional migrations and deprecation warnings.
- MAJOR: removals, incompatible schema or public ABI changes. Publish a migration guide and supported rollback path.
- Security fixes may warrant urgent release outside the regular cadence; do not delay high-severity remediation solely to preserve cadence.

## Database compatibility

1. Use **expand/contract** Flyway migrations: add nullable fields/new tables, deploy code supporting both schemas, backfill, switch application reads/writes, then remove old fields only in a later documented major release.
2. Do not rely on automatic Flyway rollback; `clean` stays disabled. Before a schema change, verify a recoverable snapshot and a tested restoration procedure.
3. Schema revisions must be compatible with the version deployed across the entire rolling upgrade window. Dual-writes or safe backfill may be required.
4. Migrations are serialized and validated; do not run multiple application instances as the owner of concurrent migrations without a defined ownership policy.
5. Postgres and Redis client version compatibility is part of the release matrix; use real service versions in CI and keep the supported minimum documented.

## Upgrade acceptance procedure

- Record artifact hashes, SBOM, OW compiler commit, Java runtime, PostgreSQL schema version and relevant third-party service versions.
- On staging data restored from a sanitized snapshot: apply the upgrade, run security and API smoke checks, validate health/readiness, verify representative job/outbox processing and review error rates.
- Run canary deployment with a clear rollback threshold; confirm the old app version can still read the new expanded schema.
- For code-only rollback, redeploy the previously signed/pinned image while retaining the compatible schema. If storage/data format changed incompatibly, halt writes and restore from the tested PITR target; quantify and approve the recovery point loss.
- Verify event deduplication, worker leases, cache invalidation and in-flight request draining. Keep operator communication and an incident timeline.
- Preserve release notes and an upgrade/rollback evidence record. Do not check the 1.0 gate off without an actual rehearsal.

## Support window

No EOL dates are committed until 1.0. On 1.0 release, publish a support window for each minor and a security-fix policy backed by staffing and CI/test capacity. Never imply that an unmaintained pre-1.0 branch receives guaranteed patches.

Related: [transactions](transactions.md), [deployment](deployment.md), and [release gates](release-gates.md).
