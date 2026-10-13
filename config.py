"""
Central configuration for DataVault.
Supports both the monitored database topology (PostgreSQL 3-node cluster & MongoDB 3-shard cluster)
and the separate dedicated PostgreSQL database for DataVault application data/telemetry.
Override any of these with environment variables of the same name.
"""
import os

# --- Monitored PostgreSQL Primary (Workload database: appdb) ---
POSTGRES_CONFIG = {
    "host": os.environ.get("PG_HOST", "localhost"),
    "port": int(os.environ.get("PG_PORT", 5432)),
    "user": os.environ.get("PG_USER", "admin"),
    "password": os.environ.get("PG_PASSWORD", "admin"),
    "dbname": os.environ.get("PG_DB", "appdb"),
    "connect_timeout": int(os.environ.get("PG_CONNECT_TIMEOUT", 3)),
}

# --- Monitored PostgreSQL Standby Replicas ---
POSTGRES_REPLICAS = [
    {
        "name": "pg-replica1",
        "host": os.environ.get("PG_REPLICA1_HOST", "localhost"),
        "port": int(os.environ.get("PG_REPLICA1_PORT", 5433)),
        "user": os.environ.get("PG_REPLICA1_USER", "admin"),
        "password": os.environ.get("PG_REPLICA1_PASSWORD", "admin"),
        "dbname": os.environ.get("PG_REPLICA1_DB", "appdb"),
    },
    {
        "name": "pg-replica2",
        "host": os.environ.get("PG_REPLICA2_HOST", "localhost"),
        "port": int(os.environ.get("PG_REPLICA2_PORT", 5434)),
        "user": os.environ.get("PG_REPLICA2_USER", "admin"),
        "password": os.environ.get("PG_REPLICA2_PASSWORD", "admin"),
        "dbname": os.environ.get("PG_REPLICA2_DB", "appdb"),
    },
]

# --- Monitored MongoDB Sharded Cluster (via mongos router) ---
MONGO_URI = os.environ.get("MONGO_URI", "mongodb://localhost:27017")
MONGO_DB = os.environ.get("MONGO_DB", "datavault")
MONGO_COLLECTION = os.environ.get("MONGO_COLLECTION", "events")

# --- Dedicated Separate PostgreSQL Database for DataVault Application Data ---
# Stores time-series snapshots, audit logs, and configuration independently
DATAVAULT_PG_CONFIG = {
    "host": os.environ.get("DATAVAULT_PG_HOST", "localhost"),
    "port": int(os.environ.get("DATAVAULT_PG_PORT", 5435)),
    "user": os.environ.get("DATAVAULT_PG_USER", "datavault_user"),
    "password": os.environ.get("DATAVAULT_PG_PASSWORD", "datavault_pass"),
    "dbname": os.environ.get("DATAVAULT_PG_DB", "datavault_db"),
    "connect_timeout": int(os.environ.get("DATAVAULT_PG_CONNECT_TIMEOUT", 3)),
}

# When True, attempts to use the dedicated PostgreSQL application database first.
# If PostgreSQL is not reachable, seamlessly falls back to SQLite for local demo/simulation.
USE_POSTGRES_STORAGE = os.environ.get("USE_POSTGRES_STORAGE", "true").lower() in ("true", "1", "yes")

# Fallback SQLite file path (used during simulation / demo mode when separate PostgreSQL is offline)
SQLITE_PATH = os.environ.get("DATAVAULT_SQLITE_PATH", os.path.join(
    os.path.dirname(os.path.abspath(__file__)), "datavault_history.db"
))

# How many historical snapshots to keep per metric type before pruning
MAX_SNAPSHOTS = int(os.environ.get("DATAVAULT_MAX_SNAPSHOTS", 500))

# Heuristic thresholds used by the analysis engine
SLOW_QUERY_MS_THRESHOLD = float(os.environ.get("SLOW_QUERY_MS_THRESHOLD", 100.0))
SEQ_SCAN_RATIO_THRESHOLD = float(os.environ.get("SEQ_SCAN_RATIO_THRESHOLD", 0.7))
MIN_TABLE_ROWS_FOR_INDEX_ADVICE = int(os.environ.get("MIN_TABLE_ROWS_FOR_INDEX_ADVICE", 1000))
SHARD_IMBALANCE_PCT_THRESHOLD = float(os.environ.get("SHARD_IMBALANCE_PCT_THRESHOLD", 15.0))
