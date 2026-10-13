#!/bin/bash
# =========================================================================
# DataVault Production Runbook: PostgreSQL Point-In-Time-Recovery (PITR)
# Restores data directory from a basebackup and replays WAL archive to a target time.
# =========================================================================
set -e

PGDATA="/var/lib/postgresql/data"
BACKUP_ARCHIVE="${1:-/backups/postgres/latest/base.tar.gz}"
WAL_ARCHIVE_DIR="${2:-/backups/wal_archive}"
RECOVERY_TARGET_TIME="${3:-2026-09-27 10:00:00 UTC}"

if [ ! -f "${BACKUP_ARCHIVE}" ]; then
    echo "Error: Backup archive ${BACKUP_ARCHIVE} not found!"
    exit 1
fi

echo "==> Step 1: Stopping PostgreSQL service..."
pg_ctl -D "${PGDATA}" stop -m fast || true

echo "==> Step 2: Emptying corrupted or target data directory..."
rm -rf "${PGDATA:?}"/*

echo "==> Step 3: Extracting physical basebackup archive..."
tar -xzf "${BACKUP_ARCHIVE}" -C "${PGDATA}"

echo "==> Step 4: Signaling recovery mode to PostgreSQL..."
touch "${PGDATA}/recovery.signal"

echo "==> Step 5: Configuring recovery parameters in postgresql.auto.conf..."
cat <<EOF >> "${PGDATA}/postgresql.auto.conf"
# PITR Configuration
restore_command = 'cp ${WAL_ARCHIVE_DIR}/%f %p'
recovery_target_time = '${RECOVERY_TARGET_TIME}'
recovery_target_action = 'promote'
EOF

chmod 700 "${PGDATA}"
echo "==> Step 6: PITR Configuration Complete. Start PostgreSQL to replay WAL up to ${RECOVERY_TARGET_TIME}."
