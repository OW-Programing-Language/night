#!/usr/bin/env bash
# Exercise a real PostgreSQL logical backup and restore against disposable CI DB.
# This is NOT a production PITR or multi-terabyte disaster recovery rehearsal.
set -euo pipefail
: "${NIGHT_JDBC_PASSWORD:?Set disposable PostgreSQL test credentials}"
export PGHOST=127.0.0.1
export PGPORT=5432
export PGDATABASE=night
export PGUSER=night_test
export PGPASSWORD="$NIGHT_JDBC_PASSWORD"

if ! command -v pg_dump >/dev/null || ! command -v pg_restore >/dev/null; then
  sudo apt-get update -qq
  sudo apt-get install -y -qq postgresql-client
fi

backup="$(mktemp --suffix=.dump)"
trap 'rm -f "$backup"' EXIT

psql -v ON_ERROR_STOP=1 -c 'CREATE TABLE night_restore_probe (id INTEGER PRIMARY KEY, payload TEXT NOT NULL)'
psql -v ON_ERROR_STOP=1 -c "INSERT INTO night_restore_probe VALUES (1, 'night-recovery-marker')"
pg_dump --format=custom --no-owner --no-acl --table=public.night_restore_probe --file="$backup"
test -s "$backup"
psql -v ON_ERROR_STOP=1 -c 'DROP TABLE night_restore_probe'
pg_restore --exit-on-error --no-owner --no-acl --dbname="$PGDATABASE" "$backup"
actual="$(psql -v ON_ERROR_STOP=1 --tuples-only --no-align --command='SELECT payload FROM night_restore_probe WHERE id = 1')"
test "$actual" = 'night-recovery-marker'
psql -v ON_ERROR_STOP=1 -c 'DROP TABLE night_restore_probe'
echo "PASS: disposable PostgreSQL logical dump/restore checks row integrity"
