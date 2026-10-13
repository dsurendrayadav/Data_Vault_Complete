# 🚨 PostgreSQL 3-Node Cluster Failover Demo & Runbook

This guide details the exact steps to demonstrate **high availability failover, standby promotion, and topology reconfiguration** on the PostgreSQL 3-node replication cluster.

---

## 1. Cluster Topology Overview

```
                      +-------------------+
                      |    pg-primary     | (Port: 5432, Read-Write)
                      |   Timeline: 1     |
                      +---------+---------+
                                |
             +------------------+------------------+
             |                                     |
             v                                     v
   +-------------------+                 +-------------------+
   |    pg-replica1    |                 |    pg-replica2    |
   | (Port: 5433)      |                 | (Port: 5434)      |
   | Standby / Replica |                 | Standby / Replica |
   +-------------------+                 +-------------------+
```

---

## 2. Failover Step-by-Step Demonstration

### Step 1: Confirm Baseline Health
Check that both replicas are actively streaming from `pg-primary`:
```bash
docker exec -it pg-primary psql -U admin -d appdb -c \
  "SELECT application_name, client_addr, state, replay_lag FROM pg_stat_replication;"
```
Expected output:
```text
 application_name | client_addr  |   state   | replay_lag 
------------------+--------------+-----------+------------
 pg-replica1      | 172.20.0.3   | streaming | 00:00:00.08
 pg-replica2      | 172.20.0.4   | streaming | 00:00:00.12
```

---

### Step 2: Simulate Primary Hardware Crash / Outage
Stop the primary container abruptly:
```bash
docker stop pg-primary
```
Verify the primary is down:
```bash
docker exec pg-replica1 pg_isready -h pg-primary -p 5432 || echo "Primary is DOWN!"
```

---

### Step 3: Promote `pg-replica1` to Primary Leader
Connect to `pg-replica1` and invoke the built-in promotion function:
```bash
docker exec -it pg-replica1 psql -U admin -d appdb -c "SELECT pg_promote();"
```
Verify `pg-replica1` is now in read-write mode:
```bash
docker exec -it pg-replica1 psql -U admin -d appdb -c "SELECT pg_is_in_recovery();"
```
*Output: `f` (False — meaning it is now a PRIMARY, not in recovery).*

---

### Step 4: Re-point `pg-replica2` to Follow the New Primary
`pg-replica2` was streaming from `pg-primary`. Reconfigure it to stream from `pg-replica1`:
```bash
docker exec -it pg-replica2 bash -c \
  "echo \"primary_conninfo = 'host=pg-replica1 port=5432 user=admin password=admin application_name=pg-replica2'\" >> /var/lib/postgresql/data/postgresql.auto.conf"

docker restart pg-replica2
```

---

### Step 5: Verify Write Operations on the New Leader
Insert a record directly on `pg-replica1`:
```bash
docker exec -it pg-replica1 psql -U admin -d appdb -c \
  "INSERT INTO transactions (user_id, amount, category) VALUES (101, 750.50, 'failover-test') RETURNING id, created_at;"
```

Check that `pg-replica2` successfully replicates the record:
```bash
docker exec -it pg-replica2 psql -U admin -d appdb -c \
  "SELECT * FROM transactions WHERE category = 'failover-test';"
```

---

### Step 6: Verify Streaming Replication on Promoted Leader
Query `pg_stat_replication` on `pg-replica1`:
```bash
docker exec -it pg-replica1 psql -U admin -d appdb -c \
  "SELECT application_name, client_addr, state, replay_lag FROM pg_stat_replication;"
```
*Output confirms `pg-replica2` is now connected and streaming from `pg-replica1` on Timeline 2.*

---

## 3. Automated Failover Script

To run the entire demonstration automatically:
```bash
python scripts/failover_demo.py
```

---

## 4. Recovering the Former Primary as a Standby (`pg_rewind`)

When the original `pg-primary` is restored, its WAL history may have diverged. Use `pg_rewind` to synchronize it without re-cloning the entire database:

```bash
# 1. Ensure old primary is stopped
docker stop pg-primary

# 2. Rewind old primary to match current leader timeline
docker exec -it pg-primary pg_rewind \
  --target-pgdata=/var/lib/postgresql/data \
  --source-server="host=pg-replica1 port=5432 user=admin password=admin dbname=appdb"

# 3. Put old primary in standby mode
touch /var/lib/postgresql/data/standby.signal
echo "primary_conninfo = 'host=pg-replica1 port=5432 user=admin password=admin application_name=pg-primary'" >> /var/lib/postgresql/data/postgresql.auto.conf

# 4. Restart as standby
docker start pg-primary
```
