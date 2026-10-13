"""
DataVault — Multi-Database Operations Hub & Analytics Dashboard
Built with Streamlit, psycopg2, and pymongo.

Features:
- Real-time ingestion from pg_stat_statements and MongoDB db.serverStatus()
- Slow query identifier with execution latency distributions
- Intelligent heuristic Index Advisor generating CREATE INDEX SQL
- Streaming replication health and time-series lag monitoring
- MongoDB sharded cluster document & chunk distribution analysis
- Dedicated PostgreSQL application database telemetry persistence
- Interactive Failover & PITR production runbook runner
"""
import os
import sys
import time
import json
import random
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st

# Add workspace directory to path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import config
import storage
from ingestion import postgres_collector, mongo_collector
from analysis import query_analyzer, replication_monitor, shard_balance

# Page Configuration
st.set_page_config(
    page_title="DataVault — Multi-Database Operations Hub",
    page_icon="🛡️",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Custom Styling (Dark Glassmorphic Theme)
st.markdown("""
<style>
    .metric-card {
        background: rgba(13, 20, 35, 0.7);
        border: 1px solid rgba(255, 255, 255, 0.08);
        border-radius: 12px;
        padding: 1.2rem;
        margin-bottom: 1rem;
        box-shadow: 0 4px 15px rgba(0, 0, 0, 0.3);
    }
    .status-badge-healthy {
        color: #10b981;
        font-weight: 700;
        background: rgba(16, 185, 129, 0.15);
        padding: 0.25rem 0.6rem;
        border-radius: 6px;
    }
    .status-badge-warning {
        color: #f59e0b;
        font-weight: 700;
        background: rgba(245, 158, 11, 0.15);
        padding: 0.25rem 0.6rem;
        border-radius: 6px;
    }
    .status-badge-critical {
        color: #ef4444;
        font-weight: 700;
        background: rgba(239, 68, 68, 0.15);
        padding: 0.25rem 0.6rem;
        border-radius: 6px;
    }
    .stCodeBlock {
        border-radius: 8px;
    }
</style>
""", unsafe_allow_html=True)


# Initialize Session State
if "simulation_mode" not in st.session_state:
    st.session_state.simulation_mode = True
if "sim_optimized" not in st.session_state:
    st.session_state.sim_optimized = False
if "sim_balanced" not in st.session_state:
    st.session_state.sim_balanced = False
if "sim_seed_count" not in st.session_state:
    st.session_state.sim_seed_count = 0
if "last_refresh" not in st.session_state:
    st.session_state.last_refresh = time.time()


def get_simulated_metrics():
    """Returns realistic mock metrics when live databases are offline."""
    db_optimized = st.session_state.sim_optimized
    db_balanced = st.session_state.sim_balanced
    seed = st.session_state.sim_seed_count

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

    if not db_optimized:
        query_stats.insert(0, {
            "query": "SELECT * FROM transactions WHERE is_flagged = true AND created_at > NOW() - INTERVAL '30 days';",
            "calls": 850,
            "total_exec_time": 3825000.0,
            "mean_exec_time": 4500.0,
            "rows": 4200,
            "shared_blks_hit": 10000,
            "shared_blks_read": 950000
        })

    table_scan_stats = [
        {"schemaname": "public", "table_name": "events", "seq_scan": 150, "seq_tup_read": 7500000, "idx_scan": 145000, "total_scans": 145150, "estimated_rows": 850000},
        {"schemaname": "public", "table_name": "users", "seq_scan": 25, "seq_tup_read": 250000, "idx_scan": 220000, "total_scans": 220025, "estimated_rows": 10000}
    ]
    if db_optimized:
        table_scan_stats.append({"schemaname": "public", "table_name": "transactions", "seq_scan": 12, "seq_tup_read": 180000, "idx_scan": 12450, "total_scans": 12462, "estimated_rows": 15000})
    else:
        table_scan_stats.append({"schemaname": "public", "table_name": "transactions", "seq_scan": 1150, "seq_tup_read": 17250000, "idx_scan": 320, "total_scans": 1470, "estimated_rows": 15000})

    index_candidates = [
        {"table_name": "users", "indexed_columns": ["id", "email"]},
        {"table_name": "events", "indexed_columns": ["id", "user_id"]},
    ]
    if db_optimized:
        index_candidates.append({"table_name": "transactions", "indexed_columns": ["id", "is_flagged", "created_at"]})
    else:
        index_candidates.append({"table_name": "transactions", "indexed_columns": ["id"]})

    lag1 = round(random.uniform(0.12, 0.35), 3)
    lag2 = round(random.uniform(2.5, 9.5) + (22.0 if random.random() > 0.8 else 0), 3)
    replication_status = [
        {"application_name": "pg-replica1", "client_addr": "192.168.1.101", "state": "streaming", "sent_lsn": "0/30000D0", "replay_lsn": "0/30000D0", "replay_lag_seconds": lag1, "write_lag_seconds": round(lag1 * 0.8, 3)},
        {"application_name": "pg-replica2", "client_addr": "192.168.1.102", "state": "streaming", "sent_lsn": "0/30000D0", "replay_lsn": "0/2FEE100", "replay_lag_seconds": lag2, "write_lag_seconds": round(lag2 * 0.1, 3)}
    ]

    mongo_status = {
        "connections_current": 22 + random.randint(-4, 4),
        "connections_available": 10000,
        "opcounters": {"insert": 150000 + seed, "query": 245000, "update": 12000, "delete": 850, "getmore": 3400, "command": 8500},
        "mem_resident_mb": 128 + random.randint(-5, 8),
        "mem_virtual_mb": 2450,
        "uptime_seconds": 43200
    }

    shards = [
        {"id": "shard1rs", "host": "mongo-shard1:27018"},
        {"id": "shard2rs", "host": "mongo-shard2:27018"},
        {"id": "shard3rs", "host": "mongo-shard3:27018"}
    ]

    if db_balanced:
        s1 = 16800 + random.randint(-50, 50)
        s2 = 16600 + random.randint(-50, 50)
        s3 = 16600 + random.randint(-50, 50)
        chunks = {"shard1rs": 10, "shard2rs": 9, "shard3rs": 9}
    else:
        s1 = 31000 + random.randint(-100, 100) + int(seed * 0.6)
        s2 = 11000 + random.randint(-50, 50) + int(seed * 0.25)
        s3 = 8000 + random.randint(-50, 50) + int(seed * 0.15)
        chunks = {"shard1rs": 18, "shard2rs": 6, "shard3rs": 4}

    shard_dist = {
        "chunk_counts_by_shard": chunks,
        "doc_counts_by_shard": {"shard1rs": s1, "shard2rs": s2, "shard3rs": s3}
    }

    pg_payload = {
        "connected": True,
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
    mongo_payload = {
        "connected": True,
        "server_status": mongo_status,
        "shards": shards,
        "shard_distribution": shard_dist,
        "errors": {}
    }
    return pg_payload, mongo_payload


# Collect Data
pg_real = postgres_collector.collect_all()
mongo_real = mongo_collector.collect_all()
storage_info = storage.get_storage_info()

# Auto-toggle simulation mode if real DBs are offline
if not pg_real.get("connected") or not mongo_real.get("connected"):
    st.session_state.simulation_mode = True

if st.session_state.simulation_mode:
    pg_data, mongo_data = get_simulated_metrics()
else:
    pg_data, mongo_data = pg_real, mongo_real

# Store snapshot into active storage backend (dedicated Postgres or SQLite)
try:
    storage.init_db()
    storage.save_snapshot("postgres", pg_data)
    storage.save_snapshot("mongo", mongo_data)
except Exception as e:
    pass

# Perform Analysis
slow_queries = query_analyzer.rank_slow_queries(pg_data.get("query_stats", []))
index_recs = query_analyzer.recommend_indexes(
    pg_data.get("table_scan_stats", []),
    pg_data.get("index_candidates", []),
    pg_data.get("query_stats", [])
)
replication_health = replication_monitor.evaluate_replication_health(pg_data.get("replication_status", []))
shard_eval = shard_balance.evaluate_shard_balance(mongo_data.get("shard_distribution", {}))


# =========================================================================
# Sidebar Navigation & Controls
# =========================================================================
with st.sidebar:
    st.markdown("## 🛡️ **DataVault Hub**")
    st.caption("PostgreSQL Replication + MongoDB Sharding Operations")
    st.markdown("---")

    st.markdown("### 🔌 Topology Status")
    
    # Dedicated Application DB Status
    if storage_info.get("is_postgresql"):
        st.success(f"**App Storage**: PostgreSQL ({storage_info.get('postgres_target')})")
    else:
        st.warning(f"**App Storage**: SQLite Fallback")
        st.caption(f"Path: `{os.path.basename(storage_info.get('sqlite_path', ''))}`")

    # Monitored Postgres Primary
    if pg_real.get("connected"):
        st.success("**Monitored PG Primary**: Online (5432)")
    else:
        st.error("**Monitored PG Primary**: Offline")

    # Monitored MongoDB Router
    if mongo_real.get("connected"):
        st.success("**Monitored MongoDB**: Online (27017)")
    else:
        st.error("**Monitored MongoDB**: Offline")

    st.markdown("---")
    st.markdown("### ⚙️ Operating Mode")
    sim_toggle = st.toggle("Simulation / Demo Mode", value=st.session_state.simulation_mode)
    if sim_toggle != st.session_state.simulation_mode:
        st.session_state.simulation_mode = sim_toggle
        st.rerun()

    if st.session_state.simulation_mode:
        st.info("💡 Running in Simulation Mode with realistic generated workloads.")
        col_s1, col_s2 = st.columns(2)
        with col_s1:
            if st.button("🌱 Seed 50k Events"):
                st.session_state.sim_seed_count += 50000
                st.session_state.sim_balanced = False
                st.success("Seeded 50k events into Mongo!")
                st.rerun()
        with col_s2:
            if st.button("⚖️ Balance Shards"):
                st.session_state.sim_balanced = True
                st.success("Balanced Mongo shards!")
                st.rerun()
        
        if not st.session_state.sim_optimized:
            if st.button("⚡ Apply Index Advice", use_container_width=True):
                st.session_state.sim_optimized = True
                st.success("Index created on transactions table!")
                st.rerun()
        else:
            if st.button("🔄 Reset Indexes", use_container_width=True):
                st.session_state.sim_optimized = False
                st.rerun()

    st.markdown("---")
    if st.button("🔄 Poll Telemetry Now", use_container_width=True):
        st.session_state.last_refresh = time.time()
        st.rerun()


# =========================================================================
# Main Dashboard Content & Navigation Tabs
# =========================================================================
st.title("🛡️ DataVault Database Operations Dashboard")
st.markdown("Multi-database operations monitor, automated query optimizer, and replication-health analytics across **PostgreSQL** and **MongoDB**.")

# Top Metric KPI Bar
kpi1, kpi2, kpi3, kpi4 = st.columns(4)
with kpi1:
    total_docs = shard_eval.get("total_docs", 0)
    kpi1.metric("MongoDB Total Documents", f"{total_docs:,}", "Across 3 Shards")
with kpi2:
    slow_count = len(slow_queries.get("by_mean_time", []))
    kpi2.metric("Slow Queries (>100ms)", slow_count, "pg_stat_statements")
with kpi3:
    active_reps = len([r for r in replication_health if r.get("status") == "healthy"])
    kpi3.metric("Healthy PG Standbys", f"{active_reps} / {len(replication_health)}", "Streaming Replication")
with kpi4:
    res_mem = mongo_data.get("server_status", {}).get("mem_resident_mb", 0)
    kpi4.metric("MongoDB Resident Memory", f"{res_mem} MB", "mongos Router")

st.markdown("---")

tab_overview, tab_queries, tab_indexes, tab_replication, tab_shards, tab_runbooks = st.tabs([
    "📊 Cluster Topology",
    "⚡ Slow Queries",
    "💡 Index Advisor",
    "🔁 Replication Health",
    "🧩 Shard Balance",
    "📖 Runbooks & Failover Demo"
])

# -------------------------------------------------------------------------
# Tab 1: Cluster Topology & Status
# -------------------------------------------------------------------------
with tab_overview:
    st.subheader("Cluster Node Architecture & Connectivity")
    
    col_t1, col_t2 = st.columns([1, 1])
    with col_t1:
        st.markdown("#### 🐘 PostgreSQL 3-Node Replication Topology")
        pg_nodes = [
            {"Node": "pg-primary (Leader)", "Role": "Primary / Read-Write", "Port": "5432", "Status": "Streaming", "Database": "appdb"},
            {"Node": "pg-replica1 (Standby 1)", "Role": "Physical Standby / Read-Only", "Port": "5433", "Status": "Connected", "Database": "appdb"},
            {"Node": "pg-replica2 (Standby 2)", "Role": "Physical Standby / Read-Only", "Port": "5434", "Status": "Connected", "Database": "appdb"},
        ]
        st.dataframe(pd.DataFrame(pg_nodes), use_container_width=True)

        st.markdown("#### 💾 Dedicated Application Telemetry Database")
        st.info(f"""
        - **Target Engine**: PostgreSQL (Dedicated Instance)
        - **Service / Host**: `{config.DATAVAULT_PG_CONFIG['host']}:{config.DATAVAULT_PG_CONFIG['port']}`
        - **Database**: `{config.DATAVAULT_PG_CONFIG['dbname']}`
        - **Active Backend**: `{'PostgreSQL (Connected)' if storage_info.get('is_postgresql') else 'SQLite Fallback (Demo Mode)'}`
        - **Total Historical Snapshots Stored**: `{storage_info.get('total_snapshots', 0):,}`
        """)

    with col_t2:
        st.markdown("#### 🍃 MongoDB 3-Shard Cluster Topology")
        mongo_nodes = [
            {"Component": "mongo-router (mongos)", "Role": "Query Router", "Port": "27017", "Status": "Active"},
            {"Component": "mongo-cfg (ReplSet cfgrs)", "Role": "Config Server", "Port": "27019", "Status": "Active"},
            {"Component": "mongo-shard1 (ReplSet shard1rs)", "Role": "Data Shard 1", "Port": "27018", "Status": "Active"},
            {"Component": "mongo-shard2 (ReplSet shard2rs)", "Role": "Data Shard 2", "Port": "27018", "Status": "Active"},
            {"Component": "mongo-shard3 (ReplSet shard3rs)", "Role": "Data Shard 3", "Port": "27018", "Status": "Active"},
        ]
        st.dataframe(pd.DataFrame(mongo_nodes), use_container_width=True)

        server_status = mongo_data.get("server_status", {})
        st.markdown(f"""
        - **Connections Active**: `{server_status.get('connections_current', 0)}` / `{server_status.get('connections_available', 0):,}`
        - **Virtual Memory**: `{server_status.get('mem_virtual_mb', 0)} MB`
        - **Uptime**: `{server_status.get('uptime_seconds', 0) // 3600} hours`
        """)

# -------------------------------------------------------------------------
# Tab 2: Slow Query Identifier
# -------------------------------------------------------------------------
with tab_queries:
    st.subheader("⚡ Slow Query Profiler (`pg_stat_statements`)")
    st.markdown("Surfaces the heaviest and most frequent queries impacting PostgreSQL performance.")

    queries_list = slow_queries.get("by_total_time", [])
    if queries_list:
        df_queries = pd.DataFrame(queries_list)
        df_queries["mean_exec_time_ms"] = df_queries["mean_exec_time"].round(2)
        df_queries["total_exec_time_s"] = (df_queries["total_exec_time"] / 1000.0).round(2)

        # Plot latency vs calls
        fig_scatter = px.scatter(
            df_queries,
            x="calls",
            y="mean_exec_time_ms",
            size="total_exec_time_s",
            color="mean_exec_time_ms",
            hover_name="query",
            title="Query Latency vs Frequency (Bubble size = Total Time)",
            labels={"calls": "Total Call Count", "mean_exec_time_ms": "Mean Latency (ms)"},
            color_continuous_scale="Reds"
        )
        st.plotly_chart(fig_scatter, use_container_width=True)

        st.markdown("#### Ranked Query Execution Profiles")
        st.dataframe(
            df_queries[["query", "calls", "mean_exec_time_ms", "total_exec_time_s", "rows"]].rename(columns={
                "query": "SQL Statement",
                "calls": "Calls",
                "mean_exec_time_ms": "Mean (ms)",
                "total_exec_time_s": "Total Time (s)",
                "rows": "Rows Returned"
            }),
            use_container_width=True
        )
    else:
        st.info("No query profiling statements detected. Ensure `pg_stat_statements` is enabled.")

# -------------------------------------------------------------------------
# Tab 3: Intelligent Index Advisor
# -------------------------------------------------------------------------
with tab_indexes:
    st.subheader("💡 PostgreSQL Missing Index Advisor")
    st.markdown("Analyzes sequential-scan heavy tables (`pg_stat_user_tables`) and parses filters in tracked queries to synthesize index candidates.")

    if index_recs:
        for rec in index_recs:
            badge_color = "🟢 HIGH" if rec.get("confidence") == "high" else "🟡 LOW"
            with st.expander(f"📌 Table: **{rec['table']}** — Seq Scan Ratio: **{int(rec['seq_scan_ratio']*100)}%** ({badge_color} CONFIDENCE)", expanded=True):
                st.write(rec["note"])
                col_i1, col_i2 = st.columns(2)
                col_i1.metric("Estimated Table Rows", f"{rec['estimated_rows']:,}")
                col_i2.metric("Sequential Scans", f"{rec['seq_scan_count']:,}")

                if rec.get("suggested_sql"):
                    st.markdown("**Recommended DDL Statement:**")
                    st.code(rec["suggested_sql"], language="sql")
                elif rec.get("suggested_columns"):
                    cols_str = ", ".join(rec['suggested_columns'])
                    ddl = f"CREATE INDEX CONCURRENTLY idx_{rec['table']}_{'_'.join(rec['suggested_columns'])} ON {rec['table']} ({cols_str});"
                    st.markdown("**Recommended DDL Statement:**")
                    st.code(ddl, language="sql")
    else:
        st.success("🎉 All table access patterns are optimal! No high-sequential scan penalties detected on large tables.")

# -------------------------------------------------------------------------
# Tab 4: Replication Health & Lag Monitor
# -------------------------------------------------------------------------
with tab_replication:
    st.subheader("🔁 Streaming Replication Health & Lag Analysis")
    st.markdown("Monitors WAL stream replay and write lag from the primary database to connected standbys.")

    # Status Cards for Standby Replicas
    rep_cols = st.columns(len(replication_health) if replication_health else 1)
    for idx, rep in enumerate(replication_health):
        with rep_cols[idx]:
            status = rep.get("status", "unknown")
            color_class = "status-badge-healthy" if status == "healthy" else ("status-badge-warning" if status == "warning" else "status-badge-critical")
            st.markdown(f"""
            <div class="metric-card">
                <h3>{rep.get('replica')}</h3>
                <p>State: <strong>{rep.get('state')}</strong></p>
                <p>Replay Lag: <span class="{color_class}">{rep.get('replay_lag_seconds', 0):.2f}s</span></p>
                <p>Write Lag: <strong>{rep.get('write_lag_seconds', 0):.2f}s</strong></p>
                <p>Status: <span class="{color_class}">{status.upper()}</span></p>
            </div>
            """, unsafe_allow_html=True)

    # Time Series Replication Lag Chart
    st.markdown("#### Replication Lag Trend Over Time")
    history_res = storage.get_history("postgres", limit=60)
    
    lag_points = []
    if history_res:
        for snap in history_res:
            ts = pd.to_datetime(snap["ts"], unit="s")
            for r in snap.get("payload", {}).get("replication_status", []):
                lag_points.append({
                    "Timestamp": ts,
                    "Replica": r.get("application_name") or r.get("client_addr") or "Standby",
                    "Lag (seconds)": r.get("replay_lag_seconds", 0)
                })

    if not lag_points:
        # Generate simulated lag trend
        now = time.time()
        for i in range(30):
            t = pd.to_datetime(now - (30 - i) * 15, unit="s")
            lag_points.append({"Timestamp": t, "Replica": "pg-replica1", "Lag (seconds)": round(random.uniform(0.1, 0.3), 3)})
            lag_points.append({"Timestamp": t, "Replica": "pg-replica2", "Lag (seconds)": round(random.uniform(3.0, 10.0) + (12 if i > 20 else 0), 3)})

    df_lag = pd.DataFrame(lag_points)
    fig_lag = px.line(
        df_lag,
        x="Timestamp",
        y="Lag (seconds)",
        color="Replica",
        title="Standby Replay Lag (Seconds)",
        markers=True,
        color_discrete_sequence=["#00f0ff", "#f59e0b", "#10b981"]
    )
    fig_lag.update_layout(yaxis_title="Replay Lag (Seconds)", xaxis_title="Time")
    st.plotly_chart(fig_lag, use_container_width=True)

# -------------------------------------------------------------------------
# Tab 5: MongoDB Shard Balance
# -------------------------------------------------------------------------
with tab_shards:
    st.subheader("🧩 MongoDB Sharded Cluster Distribution")
    st.markdown("Evaluates chunk and document density balance across shards in the cluster.")

    shard_list = shard_eval.get("shards", [])
    if shard_list:
        col_s1, col_s2 = st.columns([1, 1])
        with col_s1:
            st.markdown(f"**Balance Verdict**: {shard_eval.get('note', '')}")
            df_shards = pd.DataFrame(shard_list)
            st.dataframe(df_shards.rename(columns={
                "shard": "Shard ID",
                "doc_count": "Document Count",
                "pct_of_total": "% of Total",
                "deviation_from_even_pct": "Deviation from Even %"
            }), use_container_width=True)

        with col_s2:
            fig_bar = px.bar(
                df_shards,
                x="shard",
                y="doc_count",
                title="Document Distribution Across Shards",
                color="shard",
                text="doc_count",
                color_discrete_sequence=["#00f0ff", "#10b981", "#f59e0b"]
            )
            st.plotly_chart(fig_bar, use_container_width=True)

        # Chunk Distribution Chart
        chunk_counts = mongo_data.get("shard_distribution", {}).get("chunk_counts_by_shard", {})
        if chunk_counts:
            df_chunks = pd.DataFrame([{"Shard": k, "Chunks": v} for k, v in chunk_counts.items()])
            fig_pie = px.pie(
                df_chunks,
                names="Shard",
                values="Chunks",
                title="Chunk Allocation Across Shards",
                color_discrete_sequence=["#10b981", "#f59e0b", "#ef4444"]
            )
            st.plotly_chart(fig_pie, use_container_width=True)
    else:
        st.info("No documents detected in collection. Seed documents to inspect sharding distribution.")

# -------------------------------------------------------------------------
# Tab 6: Production Runbooks & Failover Demo
# -------------------------------------------------------------------------
with tab_runbooks:
    st.subheader("📖 Production Runbooks & Cluster Failover Demo")
    st.markdown("Step-by-step verified procedures for high availability, backup & recovery, and failover.")

    runbook_selection = st.radio(
        "Select Procedure Runbook:",
        ["1. PostgreSQL Primary Failover & Standby Promotion", "2. Physical Basebackup & PITR Runbook", "3. MongoDB Sharded Cluster Backup"],
        horizontal=True
    )

    if runbook_selection == "1. PostgreSQL Primary Failover & Standby Promotion":
        st.markdown("### 🚨 Primary Node Failure & Standby Promotion Guide")
        st.markdown("""
        When `pg-primary` experiences hardware failure or crashes, follow this production procedure:
        
        #### Step 1: Detect Primary Outage
        Verify primary is unresponsive:
        ```bash
        docker exec pg-replica1 pg_isready -h pg-primary -p 5432
        ```

        #### Step 2: Promote Standby Replica 1 to New Primary
        Promote `pg-replica1` to become read-write leader:
        ```bash
        docker exec pg-replica1 psql -U admin -d appdb -c "SELECT pg_promote();"
        ```

        #### Step 3: Reconfigure Replica 2 to Follow the New Primary
        Update `primary_conninfo` on `pg-replica2` to stream from `pg-replica1`:
        ```bash
        docker exec pg-replica2 bash -c "echo \\\"primary_conninfo = 'host=pg-replica1 port=5432 user=admin password=admin application_name=pg-replica2'\\\" >> /var/lib/postgresql/data/postgresql.auto.conf"
        docker restart pg-replica2
        ```

        #### Step 4: Verify Replication to New Leader
        Confirm standby 2 is now streaming from standby 1:
        ```bash
        docker exec pg-replica1 psql -U admin -d appdb -c "SELECT application_name, client_addr, state, replay_lag FROM pg_stat_replication;"
        ```
        """)

    elif runbook_selection == "2. Physical Basebackup & PITR Runbook":
        st.markdown("### 🗄️ PostgreSQL Point-In-Time-Recovery (PITR) Runbook")
        st.markdown("""
        Point-In-Time Recovery restores the database cluster up to a specific timestamp or transaction LSN.
        
        #### Step 1: Take Consistent Physical Basebackup
        ```bash
        PGPASSWORD=admin pg_basebackup -h localhost -p 5432 -U admin -D /var/backups/pg_base -Ft -z -P -Xs
        ```

        #### Step 2: Configure WAL Archiving
        In `postgresql.conf`:
        ```ini
        archive_mode = on
        archive_command = 'cp %p /var/backups/wal_archive/%f'
        ```

        #### Step 3: Perform Restoration to Target Timestamp
        1. Stop PostgreSQL service: `docker stop pg-primary`
        2. Clean existing data directory and extract basebackup:
        ```bash
        tar -xzf /var/backups/pg_base/base.tar.gz -C /var/lib/postgresql/data/
        ```
        3. Touch `recovery.signal` in data directory:
        ```bash
        touch /var/lib/postgresql/data/recovery.signal
        ```
        4. Configure recovery target in `postgresql.conf`:
        ```ini
        restore_command = 'cp /var/backups/wal_archive/%f %p'
        recovery_target_time = '2026-09-27 10:30:00 UTC'
        recovery_target_action = 'promote'
        ```
        5. Restart PostgreSQL to replay WAL up to the target timestamp!
        """)

    else:
        st.markdown("### 🍃 MongoDB Sharded Cluster Consistent Backup")
        st.markdown("""
        #### Step 1: Disable the Cluster Balancer
        ```bash
        docker exec mongo-router mongosh --port 27017 --eval 'sh.stopBalancer();'
        ```

        #### Step 2: Backup the Config Database
        ```bash
        docker exec mongo-cfg mongodump --port 27019 --db config --out /data/backup/config
        ```

        #### Step 3: Backup Collection Data across Shards with Oplog
        ```bash
        docker exec mongo-router mongodump --port 27017 --db datavault --collection events --oplog --out /data/backup/datavault
        ```

        #### Step 4: Re-enable Balancer
        ```bash
        docker exec mongo-router mongosh --port 27017 --eval 'sh.startBalancer();'
        ```
        """)
