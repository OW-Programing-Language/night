#!/usr/bin/env bash
# Disposable CI logical dump/restore with same PostgreSQL major client+server.
# This is not a production point-in-time recovery (PITR) drill.
set -euo pipefail
: "${NIGHT_JDBC_PASSWORD:?Set disposable PostgreSQL test credentials}"

docker run --rm --network host \
  -e PGHOST=127.0.0.1 -e PGPORT=5432 -e PGDATABASE=night -e PGUSER=night_test \
  -e PGPASSWORD="$NIGHT_JDBC_PASSWORD" postgres:16-alpine sh -ec '
    psql -v ON_ERROR_STOP=1 -c "CREATE TABLE night_restore_probe (id INTEGER PRIMARY KEY, payload TEXT NOT NULL)"
    psql -v ON_ERROR_STOP=1 -c "INSERT INTO night_restore_probe VALUES (1, '\''night-recovery-marker'\'')"
    pg_dump --format=custom --no-owner --no-acl --table=public.night_restore_probe --file=/tmp/night-restore.dump
    test -s /tmp/night-restore.dump
    psql -v ON_ERROR_STOP=1 -c "DROP TABLE night_restore_probe"
    pg_restore --exit-on-error --no-owner --no-acl --dbname="$PGDATABASE" /tmp/night-restore.dump
    actual="$(psql -v ON_ERROR_STOP=1 --tuples-only --no-align --command="SELECT payload FROM night_restore_probe WHERE id = 1")"
    test "$actual" = "night-recovery-marker"
    psql -v ON_ERROR_STOP=1 -c "DROP TABLE night_restore_probe"
    echo "PASS: PostgreSQL 16 disposable backup/restore preserved probe data"
  '
