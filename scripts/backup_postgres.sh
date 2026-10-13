#!/bin/bash
# =========================================================================
# DataVault Production Runbook: PostgreSQL Physical Basebackup
# =========================================================================
set -e

BACKUP_DIR="${BACKUP_DIR:-/backups/postgres}"
TIMESTAMP=$(date +"%Y%m%d_%H%M%S")
TARGET_DIR="${BACKUP_DIR}/basebackup_${TIMESTAMP}"

echo "==> Creating backup directory: ${TARGET_DIR}"
mkdir -p "${TARGET_DIR}"

echo "==> Initiating physical basebackup from primary (including WAL files)..."
PGPASSWORD="${POSTGRES_PASSWORD:-admin}" pg_basebackup \
    -h "${PG_HOST:-localhost}" \
    -p "${PG_PORT:-5432}" \
    -U "${PG_USER:-admin}" \
    -D "${TARGET_DIR}" \
    -Ft \
    -z \
    -P \
    -Xs

echo "==> Basebackup completed successfully at: ${TARGET_DIR}"
echo "==> Content preview:"
ls -lh "${TARGET_DIR}"
