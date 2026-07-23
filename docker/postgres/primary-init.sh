#!/bin/bash
# Runs automatically on first init (empty data volume) via Postgres's
# docker-entrypoint-initdb.d hook. wal_level/max_wal_senders/
# max_replication_slots already default to values that support streaming
# replication on Postgres 10+, so we only need to add the replication role
# and allow replica connections in pg_hba.conf.
set -e

echo "host replication replicator all scram-sha-256" >> "$PGDATA/pg_hba.conf"

psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<-EOSQL
  CREATE ROLE replicator WITH REPLICATION LOGIN PASSWORD '${REPLICATION_PASSWORD:-replpass}';

  -- Demo table so the README's example consistency-check query works
  -- out of the box.
  CREATE TABLE IF NOT EXISTS accounts (
    id INT PRIMARY KEY,
    balance NUMERIC NOT NULL
  );
  INSERT INTO accounts (id, balance) VALUES (1, 1000.00)
    ON CONFLICT (id) DO NOTHING;
EOSQL
