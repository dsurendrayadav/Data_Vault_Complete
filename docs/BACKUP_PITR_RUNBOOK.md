# 🗄️ DataVault Backup & Point-In-Time-Recovery (PITR) Runbook

This production runbook provides end-to-end operational procedures for backup and disaster recovery across the **PostgreSQL 3-node replication cluster** and the **MongoDB 3-shard cluster**.

---

## 1. PostgreSQL Backup Architecture

PostgreSQL disaster recovery relies on two foundational layers:
1. **Physical Basebackup** (`pg_basebackup`): A consistent snapshot of the entire PostgreSQL data cluster files at a specific point in time.
2. **Write-Ahead Log (WAL) Archiving**: Continuous archiving of completed WAL segments containing all subsequent database mutations.

By combining a basebackup taken at $T_0$ with WAL segments recorded up to $T_{target}$, the cluster can be reconstructed to **any precise millisecond or transaction ID** (Point-In-Time Recovery).

---

## 2. PostgreSQL Backup Procedures

### A. WAL Archiving Configuration
Ensure the following settings are present in `postgresql.conf` on `pg-primary`:
```ini
wal_level = replica
archive_mode = on
archive_command = 'test ! -f /var/backups/wal_archive/%f && cp %p /var/backups/wal_archive/%f'
archive_timeout = 300  # Force a segment switch at least every 5 minutes
```

### B. Taking a Full Physical Basebackup
Run `pg_basebackup` with tar formatting and inline compression:
```bash
PGPASSWORD=admin pg_basebackup \
    -h localhost \
    -p 5432 \
    -U admin \
    -D /var/backups/postgres/$(date +%Y%m%d_%H%M%S) \
    -Ft \
    -z \
    -P \
    -Xs
```

*Flags breakdown:*
- `-Ft`: Output format is tar files (`base.tar.gz` and separate tablespace archives).
- `-z`: Gzip compression enabled.
- `-P`: Show progress bar and estimated rows.
- `-Xs`: Stream required WAL logs in parallel so the backup is immediately self-consistent.

---

## 3. PostgreSQL Point-In-Time Recovery (PITR) Execution

### Scenario: Accidental Table Drop or Corrupt Transaction at 10:45:00 UTC
*Goal: Restore the database state exactly as it existed at 10:44:50 UTC (10 seconds before the drop).*

### Step 1: Stop PostgreSQL Service
```bash
docker compose stop pg-primary
# Or in systemd: systemctl stop postgresql
```

### Step 2: Preserve Current Corrupted State for Forensics
```bash
mv /var/lib/postgresql/data /var/lib/postgresql/data_corrupted_backup
mkdir -p /var/lib/postgresql/data
chmod 700 /var/lib/postgresql/data
```

### Step 3: Extract the Prior Physical Basebackup
```bash
tar -xzf /var/backups/postgres/20260927_000000/base.tar.gz -C /var/lib/postgresql/data/
```

### Step 4: Signal Recovery Mode
PostgreSQL 12+ requires a `recovery.signal` trigger file in `$PGDATA`:
```bash
touch /var/lib/postgresql/data/recovery.signal
```

### Step 5: Configure PITR Recovery Parameters
Append the restore configuration to `/var/lib/postgresql/data/postgresql.auto.conf`:
```ini
# Fetch archived WAL segments from the backup repository
restore_command = 'cp /var/backups/wal_archive/%f %p'

# Target restoration timestamp (ISO 8601)
recovery_target_time = '2026-09-27 10:44:50 UTC'

# Action upon reaching target: 'promote' to become standalone read-write leader
recovery_target_action = 'promote'
```

### Step 6: Start PostgreSQL and Monitor Replay
```bash
docker compose start pg-primary
docker logs -f pg-primary
```
Look for log messages:
```text
LOG: starting point-in-time recovery to 2026-09-27 10:44:50+00
LOG: restored log file "000000010000000000000002" from archive
LOG: recovery stopping before commit of transaction 84501, time 2026-09-27 10:45:01
LOG: redo done at 0/3004810
LOG: selected new timeline ID: 2
LOG: database system is ready to accept connections
```

---

## 4. MongoDB Sharded Cluster Consistent Backup

In a sharded cluster, taking an uncoordinated snapshot produces an inconsistent state because chunks are continuously migrating between shards.

### Step 1: Stop the Balancer
Prevent chunk migrations during the dump:
```bash
docker exec mongo-router mongosh --port 27017 --quiet --eval 'sh.stopBalancer();'
```

### Step 2: Backup the Config Database
The `config` database holds collection schemas and shard key ranges:
```bash
docker exec mongo-cfg mongodump --port 27019 --db config --out /data/backup/config_$(date +%F)
```

### Step 3: Backup Collection Data with Oplog
Capture mutations occurring during the dump:
```bash
docker exec mongo-router mongodump --port 27017 --db datavault --collection events --oplog --out /data/backup/data_$(date +%F)
```

### Step 4: Resume the Balancer
```bash
docker exec mongo-router mongosh --port 27017 --quiet --eval 'sh.startBalancer();'
```

### Step 5: Restoration
Restore the collection and replay the oplog:
```bash
docker exec mongo-router mongorestore --port 27017 --oplogReplay /data/backup/data_2026-09-27/
```
