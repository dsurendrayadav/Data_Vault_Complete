# 🛡️ DataVault Operations Hub

**A multi-database operations dashboard with query optimizer and replication-health monitor across PostgreSQL + MongoDB.**

DataVault is a production-grade database observability platform that analyzes and monitors:
1. **3-Node PostgreSQL Replication Cluster** (`pg-primary`, `pg-replica1`, `pg-replica2`) — Ingests `pg_stat_statements`, tracks sequential vs index scans, surfaces slow queries, suggests heuristic index recommendations, and monitors streaming replication lag.
2. **3-Shard MongoDB Cluster** (`mongo-cfg`, `mongo-shard1`, `mongo-shard2`, `mongo-shard3`, `mongo-router`) — Ingests `db.serverStatus()`, monitors chunk and document balance, and detects shard distribution skew.
3. **Dedicated Application PostgreSQL Database** (`datavault-db`) — Independent PostgreSQL database dedicated to storing telemetry snapshots, query profiles, and audit logs (with seamless SQLite fallback for local demo mode).

---

## 🚀 Key Deliverables Included

| Deliverable | Location | Description |
| :--- | :--- | :--- |
| **Vagrant / Docker Compose Cluster** | `docker-compose.yml`, `Vagrantfile` | 3-node PostgreSQL replication + 3-shard MongoDB cluster + dedicated PostgreSQL application DB + web app |
| **Python Streamlit Dashboard** | `streamlit_app.py` | Full operations dashboard built with **Streamlit + psycopg2 + pymongo** |
| **FastAPI Web Dashboard** | `app.py`, `dashboard/index.html` | High-performance API server with dark glassmorphic HTML/Chart.js single-page application |
| **Slow-Query Identifier & Index Advisor** | `analysis/query_analyzer.py` | Surfaces heavy queries from `pg_stat_statements`, extracts filter columns, generates `CREATE INDEX` SQL |
| **Replication Lag & Shard Balance Graphs** | `analysis/replication_monitor.py`, `analysis/shard_balance.py` | Time-series replication lag graphs and shard document/chunk distribution charts |
| **Backup & PITR Runbook** | `docs/BACKUP_PITR_RUNBOOK.md`, `scripts/` | Comprehensive runbook for physical basebackups (`pg_basebackup`), WAL archiving, Point-In-Time Recovery, and Mongo dumps |
| **Failover Demo & Automation** | `docs/FAILOVER_DEMO.md`, `scripts/failover_demo.py` | Automated standby promotion demo (`pg_promote()`), standby re-pointing, and replication validation |

---

## ⚡ Quick Start (Demo / Simulation Mode)

You do **not** need Docker or external databases installed to run the full dashboard. DataVault automatically detects connection status and operates in **Simulation / Demo Mode** with realistic workloads.

### 1. Setup Virtual Environment & Dependencies
```bash
# 1. Activate the virtual environment
# Windows (PowerShell):
venv\Scripts\Activate.ps1
# Windows (CMD):
venv\Scripts\activate.bat
# Linux / macOS:
source venv/bin/activate

# 2. Install dependencies
pip install -r requirements.txt
```

### 2. Launch Either Dashboard Interface

#### Option A: Streamlit Python Dashboard (Recommended by project spec)
```bash
streamlit run streamlit_app.py
```
👉 Opens at: **http://localhost:8501**

#### Option B: FastAPI Single-Page Web Dashboard
```bash
python app.py
```
👉 Opens at: **http://localhost:8000**

---

## 🐳 Full Database Cluster Setup (Docker Mode)

To stand up the real multi-database cluster topology:

### 1. Start the Topology with Docker Compose
```bash
docker compose up -d
```
This boots up:
- `pg-primary`: PostgreSQL 16 primary on port `5432` (`appdb`)
- `pg-replica1`: Standby replica 1 on port `5433`
- `pg-replica2`: Standby replica 2 on port `5434`
- `datavault-db`: Dedicated PostgreSQL database on port `5435` (`datavault_db`)
- `mongo-cfg`: MongoDB config server on port `27019`
- `mongo-shard1`, `mongo-shard2`, `mongo-shard3`: Three data shards on port `27018`
- `mongo-router`: MongoDB mongos query router on port `27017`
- `datavault-app`: Containerized dashboard web application

### 2. Initialize the MongoDB Sharded Cluster
```bash
# On Linux/macOS:
chmod +x mongo/init-cluster.sh mongo/seed-data.sh
./mongo/init-cluster.sh
./mongo/seed-data.sh
```

---

## 💻 Virtual Machine Setup (Vagrant Mode)

To run the complete environment inside an isolated Ubuntu 22.04 LTS virtual machine:
```bash
vagrant up
```
Vagrant automatically provisions Docker, pulls the required containers, and forwards all database and dashboard ports to `localhost`.

---

## 🗄️ Dedicated PostgreSQL Application Database

DataVault uses a dedicated PostgreSQL database (`datavault_db` on port `5435`) to persist telemetry snapshots, independent of the monitored database:
- **Host**: `localhost` (or `datavault-db` in Docker)
- **Port**: `5435`
- **Database**: `datavault_db`
- **User**: `datavault_user`
- **Password**: `datavault_pass`
- **Initialization Script**: `postgres/app/init_datavault_db.sql`

*Fallback Mechanism*: If the dedicated PostgreSQL container is stopped or offline, `storage.py` gracefully logs a notice and switches to local SQLite (`datavault_history.db`) with zero downtime.

---

## 🚨 Primary Failover Demonstration

Run the automated failover test script:
```bash
python scripts/failover_demo.py
```
See the full walkthrough in [`docs/FAILOVER_DEMO.md`](docs/FAILOVER_DEMO.md).

---

## 📋 Backup & PITR Procedures

Detailed step-by-step instructions for physical basebackups and point-in-time-recovery are documented in [`docs/BACKUP_PITR_RUNBOOK.md`](docs/BACKUP_PITR_RUNBOOK.md).

---

## 📁 Repository Structure

```text
Data-Vault/
├── app.py                     # FastAPI server & REST API
├── streamlit_app.py           # Streamlit Python operations dashboard
├── config.py                  # Cluster & dedicated DB configuration
├── storage.py                 # Telemetry persistence (PostgreSQL + SQLite fallback)
├── collector_daemon.py        # Standalone polling daemon
├── requirements.txt           # Python package requirements
├── docker-compose.yml         # 10-service Docker cluster configuration
├── Dockerfile                 # Application container build definition
├── Vagrantfile                # Automated Ubuntu VM provisioning
│
├── analysis/
│   ├── query_analyzer.py      # Slow query ranking & heuristic index advisor
│   ├── replication_monitor.py # Streaming replication lag evaluator
│   └── shard_balance.py       # Shard document & chunk skew evaluator
│
├── ingestion/
│   ├── postgres_collector.py  # pg_stat_statements & pg_stat_replication collector
│   └── mongo_collector.py     # db.serverStatus() & shard distribution collector
│
├── dashboard/
│   └── index.html             # Glassmorphic single-page frontend (Chart.js)
│
├── postgres/
│   ├── primary/               # Primary configuration, pg_hba, init.sh
│   ├── replica/               # Standby entrypoint & basebackup clone script
│   └── app/                   # Dedicated application PostgreSQL init SQL
│
├── mongo/
│   ├── init-cluster.sh        # Sharded cluster initiation script
│   └── seed-data.sh           # 50,000 document seeding script
│
├── scripts/
│   ├── failover_demo.py       # Automated failover promotion test script
│   ├── backup_postgres.sh     # Physical basebackup automation script
│   ├── pitr_restore.sh        # Point-In-Time-Recovery script
│   └── backup_mongo.sh        # MongoDB consistent dump script
│
└── docs/
    ├── BACKUP_PITR_RUNBOOK.md # Production backup & PITR runbook
    └── FAILOVER_DEMO.md       # Standby failover & topology reconfiguration guide
```
