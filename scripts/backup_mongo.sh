#!/bin/bash
# =========================================================================
# DataVault Production Runbook: MongoDB Sharded Cluster Backup
# Stops balancer, creates consistent dump of config server & collection oplog,
# then resumes balancer.
# =========================================================================
set -e

BACKUP_DIR="${BACKUP_DIR:-/backups/mongodb}"
TIMESTAMP=$(date +"%Y%m%d_%H%M%S")
TARGET_DIR="${BACKUP_DIR}/mongodump_${TIMESTAMP}"
ROUTER_HOST="${MONGO_ROUTER_HOST:-localhost}"
ROUTER_PORT="${MONGO_ROUTER_PORT:-27017}"

mkdir -p "${TARGET_DIR}"

echo "==> Step 1: Stopping the MongoDB Cluster Balancer..."
docker exec mongo-router mongosh --port "${ROUTER_PORT}" --quiet --eval 'sh.stopBalancer();'

echo "==> Step 2: Dumping MongoDB config metadata..."
docker exec mongo-cfg mongodump --port 27019 --db config --out "${TARGET_DIR}/config"

echo "==> Step 3: Dumping datavault database with oplog..."
docker exec mongo-router mongodump --port "${ROUTER_PORT}" --db datavault --oplog --out "${TARGET_DIR}/data"

echo "==> Step 4: Re-enabling the MongoDB Cluster Balancer..."
docker exec mongo-router mongosh --port "${ROUTER_PORT}" --quiet --eval 'sh.startBalancer();'

echo "==> MongoDB Backup Completed successfully at: ${TARGET_DIR}"
