#!/bin/bash
# On first start (empty PGDATA), clones the primary with pg_basebackup using
# -R so Postgres writes standby.signal + primary_conninfo automatically —
# no manual recovery.conf wrangling needed on Postgres 12+. On every
# subsequent start, PGDATA already has that state, so this just hands off to
# the normal postgres entrypoint, which starts it as a streaming standby.
set -e

if [ -z "$(ls -A "$PGDATA" 2>/dev/null)" ]; then
  echo "replica: data dir empty, cloning from primary via pg_basebackup..."
  until PGPASSWORD="${REPLICATION_PASSWORD:-replpass}" pg_basebackup \
      -h "${PRIMARY_HOST:-postgres-primary}" \
      -D "$PGDATA" \
      -U "${REPLICATION_USER:-replicator}" \
      -Fp -Xs -R -P
  do
    echo "replica: primary not ready yet, retrying in 2s..."
    sleep 2
  done
  chmod 0700 "$PGDATA"
  echo "replica: clone complete."
fi

exec docker-entrypoint.sh postgres
