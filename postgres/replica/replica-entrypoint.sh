#!/bin/bash
set -e

PGDATA_DIR="/var/lib/postgresql/data"

# Only clone from primary if this replica hasn't been initialized yet
if [ -z "$(ls -A "$PGDATA_DIR" 2>/dev/null)" ]; then
    echo "Replica data directory empty. Cloning from primary..."

    # Wait for the primary to be ready before attempting basebackup
    until pg_isready -h pg-primary -p 5432 -U admin; do
        echo "Waiting for primary to be ready..."
        sleep 2
    done

    # Run basebackup as postgres user
    PGPASSWORD=admin pg_basebackup \
        -h pg-primary \
        -D "$PGDATA_DIR" \
        -U admin \
        -Fp -Xs -P -R

    # Explicitly set application_name in primary_conninfo so pg_stat_replication identifies this replica by name
    REPLICA_NAME="${HOSTNAME:-pg-replica}"
    echo "primary_conninfo = 'host=pg-primary port=5432 user=admin password=admin application_name=${REPLICA_NAME}'" >> "$PGDATA_DIR/postgresql.auto.conf"

    chmod 700 "$PGDATA_DIR"
    chown -R postgres:postgres "$PGDATA_DIR"
fi

exec docker-entrypoint.sh postgres
