# Night Redis distributed cache (experimental)

This starter provides a shared Redis string cache, unlike the process-local `NightCache` service.

## Configuration

- `NIGHT_REDIS_URL` must be a `rediss://` URI for TLS. Use a scoped Redis ACL account and store credentials in a secret manager.
- `NIGHT_REDIS_NAMESPACE` is an application key prefix (defaults to `night`); only letters, digits, underscore and dash are supported.
- For **loopback-only integration tests**, `NIGHT_REDIS_ALLOW_INSECURE_LOOPBACK=true` permits `redis://127.0.0.1:6379`. This bypass must never be enabled on untrusted hosts or production environments.

```ow
import "../../src/modules/data/NightRedis.ow"
set redis: new NightRedis()
call: redis.open()
call: redis.put("session:123", "cached-value", 60)
set value: redis.get("session:123")
call: redis.evict("session:123")
```

Cache values are strings (maximum 1 MiB). TTL must be between 1 and 86400 seconds; keys are limited to safe ASCII identifiers. Missing entries return the empty string, so store a structured sentinel if empty values must be distinguished. `evict()` is a shared Redis deletion, visible to other processes using the same namespace.

Do not store access tokens or other sensitive data without an appropriate data-retention, encryption and Redis access-control policy. Keys for different tenants need enforced tenant-aware prefixes at the service layer; the current namespace is only app-level, **not** a tenant isolation boundary.

## Verification

`NightRedisTest` checks TLS settings, TTL bounds and cache operations. The real-Redis GitHub Actions service exercises cache put/get/delete with `redis:7-alpine` on loopback. The successful CI run remains to be verified. No distributed invalidation events, Redis Streams transport, pub/sub, or Redis cluster failover testing are implemented.

The main Spring Data Redis parity checkbox stays open until real production configuration, failover, keyspace policies, pub/sub and operator tests are complete.
