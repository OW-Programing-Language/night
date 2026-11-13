# Night production deployment starter (experimental)

These are **templates**, not a certified 1.0 deployment. They use the Night production example and the optional Java platform extension. Kubernetes and systemd manifests require environment-specific preparation, observability, and successful validation before use.

## Docker / Compose / HTTPS

1. Configure an internet-resolvable hostname pointing to this server; open TCP 80/443 for Caddy ACME issuance. Set `NIGHT_DOMAIN` to that hostname.
2. Generate a strong PostgreSQL password and **random** bearer tokens. Put only the SHA-256 hashes of high-entropy bearer tokens in `NIGHT_BEARER_KEYS`. Keep them in an external secrets manager or an untracked environment file; do not commit secrets.
3. Export the environment variables below, then start the sample stack:

```sh
export NIGHT_DOMAIN=night.example.com
export NIGHT_DB_PASSWORD='replace-with-secret-manager-password'
export NIGHT_BEARER_KEYS='ADMIN:REPLACE_WITH_64_HEX_DIGEST;READER:REPLACE_WITH_64_HEX_DIGEST'
docker compose config --quiet
docker compose up --build -d
```

The example containers use a non-root OW runtime, a dedicated PostgreSQL network, non-published application port, a read-only root filesystem and an HTTPS Caddy reverse proxy. The operator must enable external backups and monitoring. Docker `environment` values may be visible to host administrators and `docker inspect`; use an appropriate secrets mechanism in sensitive environments.

**Do not use the example values above verbatim.** In real deployments, create a least-privilege application DB role instead of giving the runtime its schema-migration admin identity. The sample uses one account for simplicity.

`NIGHT_IP_QUOTA_PER_MINUTE` measures the **TCP peer**, which will be the proxy in this topology, not the actual end-user address. Set a sufficiently generous application fallback or disable it, then enforce actual client-IP-aware quotas at the trusted edge. Night ignores untrusted forwarding headers.

## JWT resource server (RS256)

Set all three variables, and call `NightSecurity.enable()`:

```sh
NIGHT_JWT_ISSUER=https://identity.example.com/realms/night
NIGHT_JWT_AUDIENCE=night-api
NIGHT_JWT_JWKS_URI=https://identity.example.com/realms/night/protocol/openid-connect/certs
```

The verifier pins the RS256 signature algorithm, requires a key ID from the **configured** HTTPS JWKS endpoint, checks the signature, issuer, audience, mandatory expiry, not-before, and subject. The trusted issuer can supply a `night_roles` JSON array with role names. Missing roles still provide `AUTHENTICATED` access, not admin rights. Do not map arbitrary external scopes or user-input strings to `ADMIN`.

The JWKS retrieval library provides caching and unknown-`kid` refresh. This implementation still requires live IdP rotation tests, signing-key compromise drills, claims policy review, and independent security verification before a full OAuth2 Resource Server gate is complete. OIDC authorization-code login, PKCE, cookies, browser sessions, logout, refresh tokens, and CSRF are **not** implemented.

## Outbound HTTP client

The OW API `NightHttp.get(url)` accepts HTTPS GET requests only to **exact** DNS names in `NIGHT_HTTP_ALLOWED_HOSTS`, for example `api.example.com,events.example.com`. It disallows redirects, userinfo, URL fragments, nonstandard ports, oversized bodies and common private/reserved DNS responses. It imposes timeouts and bounded retries for idempotent GETs.

**Important:** DNS resolution can change between checking the address and making the TLS connection. An operator-managed egress proxy/firewall is required for robust SSRF containment. Do not treat this host allowlist as a complete SSRF guarantee.

## Kubernetes

Edit `deploy/k8s/night.yaml`: replace the example image with an image you built and scanned, replace `night.example.invalid` with the public hostname, supply a TLS secret, and configure `night-runtime` externally. That Secret must provide `jdbc-url`, `jdbc-user`, `jdbc-password`, and `bearer-keys`.

The `/_night/health` probe in the example tests the interpreter's liveness, **not** true DB readiness. The protected `/_night/ready` route is not suitable for unauthenticated Kube probes as currently shipped. Implement a safe probe identity or dedicated readiness mechanism before production rollout. The app is still single-worker.

## systemd

Build the OW JAR and shaded Night JAR locally, install the repository layout to `/opt/night`, create an unprivileged `night` system account, and install `deploy/night.service` to `/etc/systemd/system/night.service`. Create an untracked root-owned `/etc/night/night.env` with `chmod 0600` and application credentials, then run:

```sh
sudo systemctl daemon-reload
sudo systemctl enable --now night.service
sudo journalctl -u night.service -f
```

Run it behind a separate HTTPS reverse proxy, with the OW port bound privately or filtered by firewall. Restart-on-failure is not a substitute for graceful request draining, crash recovery or service health checks.

## Outstanding 1.0 release conditions

The image, Compose stack and manifests must pass build, functional TLS/identity tests, restore drills, soak/load tests, graceful-shutdown checks, upgrade/rollback rehearsals, SBOM/signature verification, and independent security review. See issue #3.
