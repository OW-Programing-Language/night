# Night smart auto-configuration (PR #26)

Night uses OW's existing `dependencies.yml` format and adds an **optional
`night:` section**. OW still owns source dependencies, the lockfile and
`@alias` imports; Night merely reads starter selection and configures its
installed platform extension. No change to OW's dependency manager is needed.

## Example

```yaml
format: 1
repositories: []
dependencies: []
night:
  starters: [core, data, cache, actuator, security]
  disable: [redis]
  migrations: "filesystem:db/migration"
```

```ow
import "@night/src/modules/config/NightAutoConfig.ow"
# Example above assumes an OW alias @night has been installed;
# use relative imports if Night is checked out locally.

main:
  set boot: new NightAutoConfig()
  call: assert(boot.apply("dependencies.yml") == 1)
  # Instantiate native business functors, then use Night's existing
  # discovery if desired:
  set night: new NightApp()
  call: night.discover_native()
  call: assert(night.boot() == 1)
  serve: 8080
```

The runnable in-repository example uses **relative imports**, with no external
Git dependencies: [example manifest](../examples/autoconfig/dependencies.yml)
and [example OW application](../examples/autoconfig/main.ow).

## Rules

- Only the explicit `night.starters` entries and known OW dependency aliases
  (for example `night-security`, `night-data`, `night-redis`) select a starter.
  A single `night` dependency selects `core` only; it does not expose
  unrelated privileged services.
- `night.disable` overrides **inferred** alias selections, but putting the
  same name in explicit `starters` and `disable` is a configuration error.
- Known starters: `core`, `web`, `config`, `validation`, `cache`,
  `actuator`, `openapi`, `http`, `mail`, `scheduler`, `data`,
  `redis`, `kafka`, `rabbitmq`, `security`.
- Java-side eager starts are limited to **data (JDBC/Flyway)**, **redis**,
  **kafka**, **rabbitmq**, and **security**. These opt-in services reuse the
  existing Night adapters, checks and environment variables. Passive starters
  merely declare availability: importing/mounting actual OW classes and routes
  remains the application's responsibility.
- Optional `night.migrations` requires `data`; migrations run once from an
  explicitly configured `filesystem:` or `classpath:` location. Never
  choose migrations automatically from arbitrary source trees.
- Authentication is **not** enabled merely because Night is installed.
  When `security` is selected, it is enabled before the application handles
  requests and fails closed if required credentials are absent. Sensitive
  endpoints must still be explicitly authorized/mounted.
- Database URLs, passwords, bearer-token digests, Redis/Kafka/RabbitMQ secrets,
  and TLS settings stay in `NIGHT_*` environment variables or a secret
  manager. `dependencies.yml` must contain no credentials.
- Unknown starters, duplicate names, conflicting enable/disable settings,
  unsupported manifest versions, duplicate YAML keys, unexpected `night:`
  keys and malformed migration paths fail before any adapter starts.
- Calling `apply()` with identical contents is safe. Configuration changes
  after startup or retry after an adapter failure require a process restart.
  A failed startup closes adapters and must never proceed to `serve:`.

## Limits

This is a **bounded first-stage auto-configuration feature**, not Spring Boot
bean scanning or automatic injection. The `dependencies.yml` entries are not
automatically imported into OW; users still import modules and instantiate
functors. It is interpreter + Night platform only, not LLVM/native lowering.
Neither proxy/TLS configuration nor secure deployment can be inferred from a
manifest. Night 1.0 production-readiness gates remain open.
