# Night database and persistence capability matrix

This matrix describes features **in the code**, not aspirational Spring Data compatibility.

| Capability | PostgreSQL JDBC | H2 test DB | Redis | MongoDB | Elasticsearch / OpenSearch |
|---|---|---|---|---|---|
| Connection driver | HikariCP + JDBC | HikariCP + JDBC | Jedis pooled | Not implemented | Not implemented |
| TLS | JDBC driver supports TLS (operator configured) | Not applicable for local tests | TLS required (`rediss://`, local test override) | Not implemented | Not implemented |
| Parameter-bound read/write | Yes | Yes | Key/value API, not SQL | Not implemented | Not implemented |
| Explicit transactions | Yes, JDBC | Yes, JDBC | No multi-command transactions in Night | Not implemented | Not implemented |
| Isolation selection | READ_COMMITTED / REPEATABLE_READ / SERIALIZABLE where driver supports it | Driver-dependent | Not exposed | Not implemented | Not implemented |
| Versioned migrations | Flyway | Flyway (migration dialect must be compatible) | No | Not implemented | Not implemented |
| Pagination / optimistic update | Constrained table helper | Constrained table helper | Not applicable | Not implemented | Not implemented |
| Durable outbox | PostgreSQL-specific lease/replay | Not supported | Not supported | Not implemented | Not implemented |
| TTL cache | Application-level SQL only | Not a cache | Yes, 1–86400s | Not implemented | Not implemented |
| Namespaced keyspace | Schema/table policy is caller's responsibility | N/A | Application-wide prefix | Not implemented | Not implemented |
| Multi-tenant enforcement | Not implemented | Not implemented | Not implemented | Not implemented | Not implemented |
| Production failover tests | Not complete | Not applicable | Not complete | Not implemented | Not implemented |

## Required before marking NoSQL parity complete

- MongoDB adapter with typed document mapping, schema validation, transactions where supported, secondary indexes, retries and authentication/TLS.
- Elasticsearch/OpenSearch adapter with index lifecycle, safe typed queries, mapping changes, bulk indexing, pagination and cluster failure handling.
- Redis Streams/consumer groups and keyspace invalidation/instrumentation, distinct from basic Redis cache operations.
- Capability-negotiation API so features unsupported by a backend fail explicitly rather than silently degrading.
- Real-engine integration tests, version support policy and documented migration/failover behavior.

Night must not claim Spring Data interface equivalence from this matrix. A typed repository/ORM and NoSQL adapter ecosystem remain separate 1.0 roadmap items.
