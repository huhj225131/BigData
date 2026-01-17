#!/usr/bin/env bash
set -euo pipefail

echo "[seed] Restoring seed dump into $POSTGRES_DB as $POSTGRES_USER"

# Note: docker-entrypoint starts a temporary server and runs scripts in this directory.
# This restore is intended for a fresh/empty PGDATA.
pg_restore \
  --no-owner \
  --no-acl \
  -U "$POSTGRES_USER" \
  -d "$POSTGRES_DB" \
  /docker-entrypoint-initdb.d/seed.dump

echo "[seed] Restore complete"
