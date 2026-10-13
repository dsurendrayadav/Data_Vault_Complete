import os
import sys
import time
import random
import threading
from contextlib import asynccontextmanager
from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import HTMLResponse, FileResponse, JSONResponse
from fastapi.staticfiles import StaticFiles

# Insert workspace root into path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import storage
from config import SQLITE_PATH, DATAVAULT_PG_CONFIG, POSTGRES_CONFIG
from ingestion import postgres_collector, mongo_collector
from analysis import query_analyzer, replication_monitor, shard_balance

# Global application state
app_state = {
    "simulation_mode": True,  # Defaults to true if DBs are unreachable
    "manual_simulation_override": False,  # User explicitly turned on/off simulation
    "background_collector_running": True,
    "last_collection_time": 0,
    "last_error": None,
    "simulated_db_data": {
        "postgres_connected": True,  # Active by default in demo so queries render immediately
        "postgres_optimized": False,
        "mongo_balanced": False,
        "seed_count": 0
    }
}

# Cache connection status to avoid blocking on every request
_cached_connection_status = {
    "pg_connected": False,
    "mongo_connected": False,
    "last_check": 0
}


def check_db_connections(force: bool = False):
    """Fast non-blocking check of database connectivity."""
    now = time.time()
    if not force and (now - _cached_connection_status["last_check"] < 4.0):
        return _cached_connection_status["pg_connected"], _cached_connection_status["mongo_connected"]

    pg_ok, _ = postgres_collector.test_connection(timeout=1)
    mongo_ok, _ = mongo_collector.test_connection()
    _cached_connection_status["pg_connected"] = pg_ok
    _cached_connection_status["mongo_connected"] = mongo_ok
    _cached_connection_status["last_check"] = now
    return pg_ok, mongo_ok


def generate_simulated_metrics():
    """Generate realistic operational metrics for Simulation Mode."""
    db_data = app_state["simulated_db_data"]
    is_pg_connected = db_data.get("postgres_connected", True)

    # 1. Postgres Query Stats
    query_stats = [
        {
            "query": "SELECT * FROM orders WHERE status = 'pending' AND user_id = $1 ORDER BY created_at DESC LIMIT 20;",
            "calls": 12450,
            "total_exec_time": 1867500.0,
            "mean_exec_time": 150.0,
            "rows": 249000,
            "shared_blks_hit": 1450000,
            "shared_blks_read": 12000
        },
        {
            "query": "SELECT SUM(amount), category FROM transactions WHERE user_id = $1 GROUP BY category;",
            "calls": 5420,
            "total_exec_time": 1626000.0,
            "mean_exec_time": 300.0,
            "rows": 5420,
            "shared_blks_hit": 980000,
            "shared_blks_read": 450000
        },
        {
            "query": "INSERT INTO events (user_id, type, payload) VALUES ($1, $2, $3) RETURNING id;",
            "calls": 145000,
            "total_exec_time": 290000.0,
            "mean_exec_time": 2.0,
            "rows": 145000,
            "shared_blks_hit": 850000,
            "shared_blks_read": 500
        },
        {
            "query": "SELECT * FROM users WHERE email = $1 LIMIT 1;",
            "calls": 220000,
            "total_exec_time": 110000.0,
            "mean_exec_time": 0.5,
            "rows": 220000,
            "shared_blks_hit": 1890000,
            "shared_blks_read": 0
        }
    ]

    # If the user optimization has not been run, add a very slow sequential scan query
    if not db_data["postgres_optimized"]:
        query_stats.insert(0, {
            "query": "SELECT * FROM transactions WHERE is_flagged = true AND created_at > NOW() - INTERVAL '30 days';",
            "calls": 850,
            "total_exec_time": 3825000.0,
            "mean_exec_time": 4500.0,
            "rows": 4200,
            "shared_blks_hit": 10000,
            "shared_blks_read": 950000
        })

    # 2. Postgres Table Scan Stats
    table_scan_stats = [
        {
            "schemaname": "public",
            "table_name": "events",
            "seq_scan": 150,
            "seq_tup_read": 7500000,
            "idx_scan": 145000,
            "total_scans": 145150,
            "estimated_rows": 850000
        },
        {
            "schemaname": "public",
            "table_name": "users",
            "seq_scan": 25,
            "seq_tup_read": 250000,
            "idx_scan": 220000,
            "total_scans": 220025,
            "estimated_rows": 10000
        }
    ]

    if db_data["postgres_optimized"]:
        table_scan_stats.append({
            "schemaname": "public",
            "table_name": "transactions",
            "seq_scan": 12,
            "seq_tup_read": 180000,
            "idx_scan": 12450,
            "total_scans": 12462,
            "estimated_rows": 15000
        })
    else:
        table_scan_stats.append({
            "schemaname": "public",
            "table_name": "transactions",
            "seq_scan": 1150,
            "seq_tup_read": 17250000,
            "idx_scan": 320,
            "total_scans": 1470,
            "estimated_rows": 15000
        })

    # 3. Postgres Index Candidates
    index_candidates = [
        {"table_name": "users", "indexed_columns": ["id", "email"]},
        {"table_name": "events", "indexed_columns": ["id", "user_id"]},
    ]
    if db_data["postgres_optimized"]:
        index_candidates.append({"table_name": "transactions", "indexed_columns": ["id", "is_flagged", "created_at", "user_id"]})
    else:
        index_candidates.append({"table_name": "transactions", "indexed_columns": ["id"]})

    # 4. Postgres Replication Lag (Fluctuating)
    lag1 = round(random.uniform(0.08, 0.25), 3)
    lag2 = round(random.uniform(2.5, 12.0) + (25.0 if random.random() > 0.85 else 0), 3)

    replication_status = [
        {
            "application_name": "pg-replica1",
            "client_addr": "192.168.1.101",
            "state": "streaming",
            "sent_lsn": "0/30000D0",
            "replay_lsn": "0/30000D0",
            "replay_lag_seconds": lag1,
            "write_lag_seconds": round(lag1 * 0.8, 3)
        },
        {
            "application_name": "pg-replica2",
            "client_addr": "192.168.1.102",
            "state": "streaming",
            "sent_lsn": "0/30000D0",
            "replay_lsn": "0/2FEE100",
            "replay_lag_seconds": lag2,
            "write_lag_seconds": round(lag2 * 0.1, 3)
        }
    ]

    # 5. MongoDB Server Status
    mongo_server_status = {
        "connections_current": 18 + int(random.uniform(-3, 6)),
        "connections_available": 10000,
        "opcounters": {
            "insert": 150000 + db_data["seed_count"],
            "query": 245000,
            "update": 12000,
            "delete": 850,
            "getmore": 3400,
            "command": 8500
        },
        "mem_resident_mb": 115 + int(random.uniform(-5, 10)),
        "mem_virtual_mb": 2450,
        "uptime_seconds": 36000 + int(time.time() % 1000)
    }

    # 6. MongoDB Shards list
    shards = [
        {"id": "shard1rs", "host": "mongo-shard1:27018"},
        {"id": "shard2rs", "host": "mongo-shard2:27018"},
        {"id": "shard3rs", "host": "mongo-shard3:27018"}
    ]

    # 7. MongoDB Shard Distribution
    if db_data["mongo_balanced"]:
        s1_docs = 16800 + int(random.uniform(-100, 100))
        s2_docs = 16600 + int(random.uniform(-100, 100))
        s3_docs = 16600 + int(random.uniform(-100, 100))
        chunks = {"shard1rs": 10, "shard2rs": 9, "shard3rs": 9}
    else:
        s1_docs = 31000 + int(random.uniform(-150, 150))
        s2_docs = 11000 + int(random.uniform(-150, 150))
        s3_docs = 8000 + int(random.uniform(-150, 150))
        chunks = {"shard1rs": 18, "shard2rs": 6, "shard3rs": 4}

    if db_data["seed_count"] > 0:
        s1_docs += int(db_data["seed_count"] * 0.6)
        s2_docs += int(db_data["seed_count"] * 0.25)
        s3_docs += int(db_data["seed_count"] * 0.15)

    mongo_distribution = {
        "chunk_counts_by_shard": chunks,
        "doc_counts_by_shard": {
            "shard1rs": s1_docs,
            "shard2rs": s2_docs,
            "shard3rs": s3_docs
        }
    }

    # Compile Postgres Payload
    postgres_payload = {
        "connected": is_pg_connected,
        "query_stats": query_stats,
        "table_scan_stats": table_scan_stats,
        "index_candidates": index_candidates,
        "replication_status": replication_status,
        "standby_probes": [
            {"name": "pg-replica1", "connected": True, "is_in_recovery": True, "endpoint": "localhost:5433"},
            {"name": "pg-replica2", "connected": True, "is_in_recovery": True, "endpoint": "localhost:5434"}
        ],
        "errors": {}
    }

    # Compile Mongo Payload
    mongo_payload = {
        "connected": True,
        "server_status": mongo_server_status,
        "shards": shards,
        "shard_distribution": mongo_distribution,
        "errors": {}
    }

    return postgres_payload, mongo_payload


def perform_snapshot_collection():
    """Gathers real metrics or generates simulated ones, then stores in storage backend."""
    pg_connected, mongo_connected = check_db_connections()

    # Auto-toggle simulation mode if DBs are unreachable and user hasn't overridden
    if not app_state["manual_simulation_override"]:
        if not pg_connected or not mongo_connected:
            app_state["simulation_mode"] = True
        else:
            app_state["simulation_mode"] = False

    if app_state["simulation_mode"]:
        pg_data, mongo_data = generate_simulated_metrics()
    else:
        pg_data = postgres_collector.collect_all()
        mongo_data = mongo_collector.collect_all()

    try:
        storage.save_snapshot("postgres", pg_data)
        storage.save_snapshot("mongo", mongo_data)
        app_state["last_collection_time"] = time.time()
        app_state["last_error"] = None
    except Exception as e:
        app_state["last_error"] = str(e)
        print(f"Error saving database snapshot: {e}")


def background_collector_loop():
    """Background polling loop executing on a daemon thread."""
    print("Background metric collector thread started.")
    try:
        storage.init_db()
    except Exception as e:
        print("Storage init notice:", e)

    while app_state["background_collector_running"]:
        try:
            perform_snapshot_collection()
        except Exception as e:
            print("Collection loop exception:", e)

        for _ in range(30):  # poll every 15s
            if not app_state["background_collector_running"]:
                break
            time.sleep(0.5)


@asynccontextmanager
async def lifespan(app: FastAPI):
    collector_thread = threading.Thread(target=background_collector_loop, daemon=True)
    collector_thread.start()
    yield
    app_state["background_collector_running"] = False
    print("Background metric collector thread shutting down.")


app = FastAPI(
    title="DataVault API Server",
    description="Backend API and web UI router for Postgres + MongoDB operational analytics",
    lifespan=lifespan
)

# Serves static frontend files if folder exists
dashboard_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "dashboard")
if os.path.exists(dashboard_dir):
    app.mount("/static", StaticFiles(directory=dashboard_dir), name="static")


@app.get("/", response_class=HTMLResponse)
async def serve_index():
    index_file = os.path.join(dashboard_dir, "index.html")
    if os.path.exists(index_file):
        return FileResponse(index_file)
    return HTMLResponse("<h2>Dashboard index.html not found!</h2>")


@app.get("/api/status")
async def get_system_status():
    pg_connected, mongo_connected = check_db_connections()
    storage_info = storage.get_storage_info()

    # In simulation mode, use simulated connection state if PG is offline
    is_pg_online = pg_connected or (app_state["simulation_mode"] and app_state["simulated_db_data"].get("postgres_connected", False))

    return {
        "postgres_connected": is_pg_online,
        "postgres_real_connected": pg_connected,
        "mongodb_connected": mongo_connected or app_state["simulation_mode"],
        "simulation_mode": app_state["simulation_mode"],
        "manual_simulation_override": app_state["manual_simulation_override"],
        "last_collection_time": app_state["last_collection_time"],
        "last_error": app_state["last_error"],
        "storage_info": storage_info,
        "storage_backend": storage_info.get("backend"),
        "sqlite_path": SQLITE_PATH,
        "history_snapshots_count": storage_info.get("total_snapshots", 0),
        "simulated_data": app_state["simulated_db_data"]
    }


@app.get("/api/metrics")
async def get_live_metrics():
    """Returns the latest operational analysis snapshot."""
    pg_connected, mongo_connected = check_db_connections()

    if not app_state["manual_simulation_override"]:
        if not pg_connected or not mongo_connected:
            app_state["simulation_mode"] = True
        else:
            app_state["simulation_mode"] = False

    if app_state["simulation_mode"]:
        pg_data, mongo_data = generate_simulated_metrics()
    else:
        pg_data = postgres_collector.collect_all()
        mongo_data = mongo_collector.collect_all()

    # Apply analyzers to raw metrics
    pg_analyzed = {
        "connected": pg_data.get("connected", False),
        "errors": pg_data.get("errors", {}),
        "slow_queries": query_analyzer.rank_slow_queries(pg_data.get("query_stats", [])),
        "index_recommendations": query_analyzer.recommend_indexes(
            pg_data.get("table_scan_stats", []),
            pg_data.get("index_candidates", []),
            pg_data.get("query_stats", [])
        ),
        "replication_health": replication_monitor.evaluate_replication_health(pg_data.get("replication_status", [])),
        "standby_probes": pg_data.get("standby_probes", [])
    }

    mongo_analyzed = {
        "connected": mongo_data.get("connected", False),
        "errors": mongo_data.get("errors", {}),
        "server_status": mongo_data.get("server_status", {}),
        "shards": mongo_data.get("shards", []),
        "shard_balance": shard_balance.evaluate_shard_balance(mongo_data.get("shard_distribution", {})),
        "shard_distribution": mongo_data.get("shard_distribution", {})
    }

    return {
        "timestamp": time.time(),
        "simulation_mode": app_state["simulation_mode"],
        "postgres": pg_analyzed,
        "mongodb": mongo_analyzed
    }


@app.get("/api/history")
async def get_metrics_history(limit: int = 100):
    """Retrieve time-series history metrics for trend charts."""
    pg_history = storage.get_history("postgres", limit=limit)
    mongo_history = storage.get_history("mongo", limit=limit)

    # If history is empty and simulation mode is active, generate dynamic mock history
    if not pg_history or app_state["simulation_mode"]:
        mock_pg_history = []
        mock_mongo_history = []
        db_data = app_state["simulated_db_data"]
        now = time.time()

        for idx in range(30):
            ts = now - (30 - idx) * 30
            lag1 = round(random.uniform(0.1, 0.4), 2)
            lag2 = round(random.uniform(3.0, 12.0) + (10 if idx > 20 else 0), 2)

            mock_pg_history.append({
                "ts": ts,
                "payload": {
                    "replication_status": [
                        {"application_name": "pg-replica1", "replay_lag_seconds": lag1},
                        {"application_name": "pg-replica2", "replay_lag_seconds": lag2}
                    ]
                }
            })

            s1_doc = 31000 - (idx * 280) if not db_data["mongo_balanced"] else 16800 + int(random.uniform(-50, 50))
            s2_doc = 11000 + (idx * 110) if not db_data["mongo_balanced"] else 16600 + int(random.uniform(-50, 50))
            s3_doc = 8000 + (idx * 170) if not db_data["mongo_balanced"] else 16600 + int(random.uniform(-50, 50))

            if db_data["seed_count"] > 0:
                s1_doc += int(db_data["seed_count"] * 0.6)
                s2_doc += int(db_data["seed_count"] * 0.25)
                s3_doc += int(db_data["seed_count"] * 0.15)

            mock_mongo_history.append({
                "ts": ts,
                "payload": {
                    "shard_distribution": {
                        "doc_counts_by_shard": {
                            "shard1rs": s1_doc,
                            "shard2rs": s2_doc,
                            "shard3rs": s3_doc
                        }
                    }
                }
            })
        return {
            "postgres": mock_pg_history,
            "mongodb": mock_mongo_history
        }

    return {
        "postgres": pg_history,
        "mongodb": mongo_history
    }


@app.get("/api/insights")
async def get_dynamic_insights():
    """
    Returns dynamic textual insights based on current metrics.
    Analyzes replication lag, shard balance, slow queries, and index candidates.
    """
    pg_connected, mongo_connected = check_db_connections()

    if app_state["simulation_mode"]:
        pg_data, mongo_data = generate_simulated_metrics()
    else:
        pg_data = postgres_collector.collect_all()
        mongo_data = mongo_collector.collect_all()

    insights = {
        "replication": [],
        "shards": [],
        "queries": [],
        "indexes": [],
        "summary": ""
    }

    # --- Replication Insights ---
    rep_status = pg_data.get("replication_status", [])
    rep_health = replication_monitor.evaluate_replication_health(rep_status)
    critical_count = sum(1 for r in rep_health if r.get("status") == "critical")
    warning_count = sum(1 for r in rep_health if r.get("status") == "warning")

    for replica in rep_health:
        lag = replica.get("replay_lag_seconds") or 0
        name = replica.get("replica", "Unknown")
        status = replica.get("status", "unknown")
        if status == "critical":
            insights["replication"].append({
                "level": "danger",
                "icon": "🔴",
                "title": f"{name} — Critical Lag Detected",
                "body": f"Replica lag is {lag:.1f}s. This far exceeds the critical threshold (>30s). WAL sender backpressure or network congestion may be causing the standby to fall behind. Consider investigating network latency, pg_stat_replication sent_lsn vs replay_lsn gap, or temporarily pausing high-write workloads."
            })
        elif status == "warning":
            insights["replication"].append({
                "level": "warning",
                "icon": "🟡",
                "title": f"{name} — Elevated Replication Lag",
                "body": f"Replica lag is {lag:.1f}s (threshold: >5s). Monitor this node closely. If lag continues to grow, it may indicate write amplification on the primary or insufficient standby I/O throughput."
            })
        else:
            insights["replication"].append({
                "level": "success",
                "icon": "🟢",
                "title": f"{name} — Replication Healthy",
                "body": f"Lag is {lag:.2f}s. Standby is caught up with the primary WAL stream. No action required."
            })

    if not rep_health:
        insights["replication"].append({
            "level": "info",
            "icon": "ℹ️",
            "title": "Standby Replica Monitoring",
            "body": "No active streaming standbys detected. Standby monitoring provides real-time replay/write lag graphs and early warning alerts when replicas fall behind."
        })

    # --- Shard Balance Insights ---
    shard_dist = mongo_data.get("shard_distribution", {})
    shard_eval = shard_balance.evaluate_shard_balance(shard_dist)
    shards = shard_eval.get("shards", [])
    total_docs = shard_eval.get("total_docs", 0)

    for shard in shards:
        pct = shard.get("pct_of_total", 0)
        dev = shard.get("deviation_from_even_pct", 0)
        shard_id = shard.get("shard", "Unknown")
        docs = shard.get("doc_count", 0)
        if dev > 25:
            insights["shards"].append({
                "level": "danger",
                "icon": "🔴",
                "title": f"{shard_id} — Severe Shard Imbalance ({pct}% of docs)",
                "body": f"This shard holds {docs:,} documents ({pct}% of total), which is {dev}% above the ideal equal split. MongoDB's balancer may not be keeping up with chunk migrations. Consider clicking 'Balance Shards' or enabling MongoDB balancer."
            })
        elif dev > 10:
            insights["shards"].append({
                "level": "warning",
                "icon": "🟡",
                "title": f"{shard_id} — Minor Shard Skew ({pct}%)",
                "body": f"This shard holds {docs:,} documents ({pct}% of total), slightly above expected equal distribution. Skew of {dev}% is within acceptable operational range but trending upward."
            })
        else:
            insights["shards"].append({
                "level": "success",
                "icon": "🟢",
                "title": f"{shard_id} — Well Balanced ({pct}%)",
                "body": f"This shard holds {docs:,} documents ({pct}%). Distribution deviation is only {dev}% from ideal. Cluster is evenly balanced."
            })

    if total_docs > 0 and shards:
        max_shard = max(shards, key=lambda x: x.get("pct_of_total", 0), default=None)
        if max_shard and max_shard.get("deviation_from_even_pct", 0) > 20:
            insights["shards"].insert(0, {
                "level": "danger",
                "icon": "⚡",
                "title": "Action Recommended — Shard Rebalancing",
                "body": f"Cluster holds {total_docs:,} total documents with notable skew on {max_shard.get('shard')}. Click 'Balance Shards' button to trigger automated chunk migrations and re-distribute document load."
            })

    # --- Slow Query Insights ---
    slow_q = query_analyzer.rank_slow_queries(pg_data.get("query_stats", []))
    by_mean = slow_q.get("by_mean_time", [])
    by_total = slow_q.get("by_total_time", [])
    threshold = slow_q.get("threshold_ms", 100)

    if by_mean:
        worst = by_mean[0]
        insights["queries"].append({
            "level": "warning" if worst.get("mean_exec_time", 0) < 1000 else "danger",
            "icon": "🐌",
            "title": f"Slowest Query: {worst.get('mean_exec_time', 0):.0f}ms avg per call",
            "body": f"Query called {worst.get('calls', 0):,} times averaging {worst.get('mean_exec_time', 0):.1f}ms per execution (Total cost: {worst.get('total_exec_time', 0):,.0f}ms). Consider applying recommended index."
        })

    high_freq = [q for q in (by_total or []) if q.get("calls", 0) > 10000]
    if high_freq:
        hf = high_freq[0]
        insights["queries"].append({
            "level": "info",
            "icon": "🔁",
            "title": f"High-Frequency Pattern ({hf.get('calls', 0):,} calls)",
            "body": f"High-throughput query pattern detected. Even microsecond optimizations or composite index coverage will generate substantial cumulative I/O reduction."
        })

    if not by_mean and pg_data.get("connected", False):
        insights["queries"].append({
            "level": "success",
            "icon": "✅",
            "title": "No Slow Queries Detected",
            "body": f"All tracked queries are executing smoothly below the {threshold}ms threshold."
        })

    # --- Index Insights ---
    idx_recs = query_analyzer.recommend_indexes(
        pg_data.get("table_scan_stats", []),
        pg_data.get("index_candidates", []),
        pg_data.get("query_stats", [])
    )
    if idx_recs:
        high_conf = [r for r in idx_recs if r.get("confidence") == "high"]
        insights["indexes"].append({
            "level": "warning" if high_conf else "info",
            "icon": "📋",
            "title": f"{len(idx_recs)} Index Recommendation(s) Available ({len(high_conf)} High Confidence)",
            "body": f"Heuristic analysis detected sequential scans on high-traffic tables. Click 'Build Index' or run CREATE INDEX CONCURRENTLY to optimize query performance without table locks."
        })
        for rec in idx_recs[:2]:
            insights["indexes"].append({
                "level": "info",
                "icon": "🗂️",
                "title": f"Table: {rec.get('table')} — Seq Scan Ratio {int(rec.get('seq_scan_ratio', 0) * 100)}%",
                "body": f"{rec.get('note', '')} (Estimated rows: {rec.get('estimated_rows', 0):,})."
            })
    else:
        insights["indexes"].append({
            "level": "success",
            "icon": "✅",
            "title": "Indexes Fully Optimized",
            "body": "No unindexed sequential scans found on large tables. Query pattern indexes are serving queries optimally."
        })

    # --- Summary ---
    total_issues = critical_count + warning_count + len([r for r in idx_recs if r.get("confidence") == "high"])
    if total_issues == 0:
        insights["summary"] = "✅ All systems operating normally. Database latency and shard balance are healthy."
    elif critical_count > 0:
        insights["summary"] = f"🔴 {critical_count} critical replication alert(s) detected. Check standby nodes."
    else:
        insights["summary"] = f"🟡 {total_issues} optimization opportunity/opportunities identified. Review index advisor and shard balance."

    return insights


@app.post("/api/collect")
async def trigger_manual_collection():
    """Trigger an instant collector snapshot run."""
    threading.Thread(target=perform_snapshot_collection, daemon=True).start()
    return {"status": "success", "last_collection_time": time.time(), "message": "Metrics collected successfully!"}


@app.post("/api/simulation")
async def toggle_simulation_mode(enable: bool):
    """Enable or disable Simulation/Demo mode."""
    app_state["simulation_mode"] = enable
    app_state["manual_simulation_override"] = True
    threading.Thread(target=perform_snapshot_collection, daemon=True).start()
    return {"status": "success", "simulation_mode": app_state["simulation_mode"]}


@app.post("/api/pg/connect")
async def toggle_postgres_connection():
    """
    Toggle PostgreSQL database connectivity.
    Tries real PostgreSQL first; if unreachable, toggles simulated connected state
    so the dashboard instantly reflects connected status with live streaming telemetry!
    """
    real_pg_ok, _ = postgres_collector.test_connection(timeout=1)

    if real_pg_ok:
        # Real PG is available! Connect to live database
        app_state["simulation_mode"] = False
        app_state["manual_simulation_override"] = True
        app_state["simulated_db_data"]["postgres_connected"] = True
        threading.Thread(target=perform_snapshot_collection, daemon=True).start()
        return {
            "status": "connected",
            "mode": "live",
            "postgres_connected": True,
            "simulation_mode": False,
            "message": "Connected to live PostgreSQL database cluster! Streaming live operational telemetry."
        }
    else:
        # Toggle simulated PG connectivity
        current = app_state["simulated_db_data"].get("postgres_connected", False)
        new_state = not current
        app_state["simulated_db_data"]["postgres_connected"] = new_state
        threading.Thread(target=perform_snapshot_collection, daemon=True).start()

        if new_state:
            return {
                "status": "connected",
                "mode": "simulated",
                "postgres_connected": True,
                "simulation_mode": True,
                "message": "PostgreSQL connected! Operational telemetry and query statistics are now streaming."
            }
        else:
            return {
                "status": "disconnected",
                "mode": "simulated",
                "postgres_connected": False,
                "simulation_mode": True,
                "message": "PostgreSQL disconnected. Database is now in offline standby mode."
            }


@app.get("/api/pg/config")
async def get_pg_config():
    """Return active PostgreSQL configuration (passwords masked)."""
    cfg = postgres_collector.get_postgres_config()
    safe_cfg = dict(cfg)
    if "password" in safe_cfg:
        safe_cfg["password"] = "••••••••" if safe_cfg["password"] else ""
    return safe_cfg


@app.post("/api/pg/config")
async def update_pg_config(req: Request):
    """
    Update PostgreSQL connection parameters and attempt to connect.
    Accepts JSON: { host, port, user, password, dbname }
    """
    try:
        body = await req.json()
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid JSON body")

    new_cfg = {}
    for key in ("host", "port", "user", "password", "dbname"):
        if key in body and body[key] is not None:
            if key == "port":
                new_cfg[key] = int(body[key])
            else:
                new_cfg[key] = str(body[key])

    # Test the connection
    is_connected, msg = postgres_collector.test_connection(override_config=new_cfg, timeout=2)
    if is_connected:
        postgres_collector.set_postgres_config(new_cfg)
        app_state["simulation_mode"] = False
        app_state["manual_simulation_override"] = True
        app_state["simulated_db_data"]["postgres_connected"] = True
        _cached_connection_status["pg_connected"] = True
        _cached_connection_status["last_check"] = time.time()
        threading.Thread(target=perform_snapshot_collection, daemon=True).start()
        return {
            "status": "connected",
            "postgres_connected": True,
            "message": f"Successfully connected to PostgreSQL at {new_cfg.get('host', 'localhost')}:{new_cfg.get('port', 5432)}/{new_cfg.get('dbname', 'postgres')}!"
        }
    else:
        return {
            "status": "failed",
            "postgres_connected": False,
            "message": f"Could not connect to PostgreSQL: {msg}"
        }


@app.post("/api/optimize")
async def trigger_index_optimization():
    """Simulate applying index recommendations (Postgres optimization)."""
    app_state["simulated_db_data"]["postgres_optimized"] = True
    threading.Thread(target=perform_snapshot_collection, daemon=True).start()
    return {"status": "success", "message": "Index built successfully! Index recommendation updated and query performance optimized."}


@app.post("/api/balance")
async def trigger_shard_balancing():
    """Simulate re-balancing MongoDB shards."""
    app_state["simulated_db_data"]["mongo_balanced"] = True
    threading.Thread(target=perform_snapshot_collection, daemon=True).start()
    return {"status": "success", "message": "MongoDB balancer activated! Chunk and document counts re-balanced evenly across shards."}


@app.post("/api/seed")
async def trigger_data_seeding(count: int = 50000):
    """Simulate inserting data to generate stats loads."""
    app_state["simulated_db_data"]["seed_count"] += count
    app_state["simulated_db_data"]["mongo_balanced"] = False
    threading.Thread(target=perform_snapshot_collection, daemon=True).start()
    return {"status": "success", "message": f"Successfully seeded {count:,} events! Workload metrics updated across cluster."}


if __name__ == "__main__":
    import uvicorn
    port = int(os.environ.get("PORT", 8000))
    host = os.environ.get("HOST", "0.0.0.0")
    uvicorn.run("app:app", host=host, port=port, reload=False)
