"""
Collects operational metrics from the PostgreSQL replication cluster:
  - Slow / expensive queries from pg_stat_statements
  - Table access patterns (sequential vs index scans) for index advisor
  - Replication lag as seen from the primary's point of view (pg_stat_replication)
  - Direct standby recovery health verification across replica nodes
"""
import socket
import psycopg2
import psycopg2.extras

from config import POSTGRES_CONFIG, POSTGRES_REPLICAS

# Mutable runtime config that can be dynamically updated via API
_active_config = dict(POSTGRES_CONFIG)
_active_replicas = list(POSTGRES_REPLICAS)


def set_postgres_config(new_config):
    """Dynamically update active PostgreSQL connection configuration."""
    global _active_config
    if new_config:
        _active_config.update(new_config)
    return _active_config


def get_postgres_config():
    """Return active PostgreSQL configuration."""
    return dict(_active_config)


def _is_port_open(host: str, port: int, timeout: float = 0.3) -> bool:
    """Fast socket check to see if a port is listening before attempting full handshake."""
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.settimeout(timeout)
        target_host = "127.0.0.1" if host in ("localhost", "::1") else host
        res = s.connect_ex((target_host, int(port)))
        s.close()
        return res == 0
    except Exception:
        return False


def _connect(override_config=None, timeout: int = 1):
    cfg = dict(override_config or _active_config)
    cfg["connect_timeout"] = timeout
    return psycopg2.connect(**cfg)


def test_connection(override_config=None, timeout: int = 1):
    """
    Test PostgreSQL connection with fast fail.
    Returns (is_connected: bool, message: str).
    """
    cfg = override_config or _active_config
    host = cfg.get("host", "localhost")
    port = int(cfg.get("port", 5432))

    if not _is_port_open(host, port, timeout=0.5):
        return False, f"PostgreSQL port {port} on {host} is closed or unreachable."

    try:
        with _connect(override_config=cfg, timeout=timeout) as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT version();")
                v = cur.fetchone()[0]
                return True, f"Connected to PostgreSQL ({v.split(',')[0]})"
    except Exception as e:
        return False, str(e)


def get_query_stats(limit: int = 25):
    """Return the most expensive queries by total execution time from pg_stat_statements."""
    sql_modern = """
        SELECT
            query,
            calls,
            total_exec_time,
            mean_exec_time,
            rows,
            shared_blks_hit,
            shared_blks_read
        FROM pg_stat_statements
        WHERE query NOT ILIKE '%pg_stat_statements%'
        ORDER BY total_exec_time DESC
        LIMIT %s;
    """
    sql_legacy = """
        SELECT
            query,
            calls,
            total_time AS total_exec_time,
            mean_time AS mean_exec_time,
            rows,
            shared_blks_hit,
            shared_blks_read
        FROM pg_stat_statements
        WHERE query NOT ILIKE '%pg_stat_statements%'
        ORDER BY total_time DESC
        LIMIT %s;
    """
    try:
        with _connect() as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                try:
                    cur.execute(sql_modern, (limit,))
                    return [dict(r) for r in cur.fetchall()], None
                except psycopg2.Error:
                    conn.rollback()
                    cur.execute(sql_legacy, (limit,))
                    return [dict(r) for r in cur.fetchall()], None
    except Exception as e:
        return [], f"Query stats notice: {str(e)}"


def get_table_scan_stats():
    """
    Return per-table scan stats: how often each table is scanned sequentially vs via index.
    """
    sql = """
        SELECT
            schemaname,
            relname AS table_name,
            seq_scan,
            seq_tup_read,
            idx_scan,
            COALESCE(idx_scan, 0) + COALESCE(seq_scan, 0) AS total_scans,
            n_live_tup AS estimated_rows
        FROM pg_stat_user_tables
        ORDER BY seq_scan DESC;
    """
    try:
        with _connect() as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute(sql)
                return [dict(r) for r in cur.fetchall()], None
    except Exception as e:
        return [], f"Table scan stats notice: {str(e)}"


def get_missing_index_candidates():
    """Look at indexed columns already tracked by Postgres itself."""
    sql = """
        SELECT
            t.relname AS table_name,
            array_agg(DISTINCT a.attname) FILTER (WHERE a.attname IS NOT NULL) AS indexed_columns
        FROM pg_stat_user_tables t
        LEFT JOIN pg_index i ON t.relid = i.indrelid
        LEFT JOIN pg_attribute a ON a.attrelid = t.relid AND a.attnum = ANY(i.indkey)
        GROUP BY t.relname;
    """
    try:
        with _connect() as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute(sql)
                return [dict(r) for r in cur.fetchall()], None
    except Exception as e:
        return [], f"Index candidates notice: {str(e)}"


def get_replication_status():
    """Query pg_stat_replication on the primary node."""
    sql = """
        SELECT
            application_name,
            client_addr::text,
            state,
            sync_state,
            COALESCE(
                EXTRACT(EPOCH FROM (now() - replay_lag)),
                0.0
            ) AS replay_lag_seconds,
            COALESCE(
                EXTRACT(EPOCH FROM (now() - write_lag)),
                0.0
            ) AS write_lag_seconds,
            sent_lsn::text,
            replay_lsn::text
        FROM pg_stat_replication;
    """
    try:
        with _connect() as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute(sql)
                rows = [dict(r) for r in cur.fetchall()]
                for r in rows:
                    for lag_field in ("replay_lag_seconds", "write_lag_seconds"):
                        if r[lag_field] is not None:
                            r[lag_field] = float(r[lag_field])
                return rows, None
    except Exception as e:
        return [], f"Replication status notice: {str(e)}"


def check_standby_replicas():
    """Quickly probe standby replica nodes."""
    replica_health = []
    for node in _active_replicas:
        name = node.get("name", "pg-replica")
        host = node.get("host", "localhost")
        port = int(node.get("port", 5433))

        if not _is_port_open(host, port, timeout=0.2):
            replica_health.append({
                "name": name,
                "connected": False,
                "is_in_recovery": None,
                "endpoint": f"{host}:{port}",
                "error": "Port closed / unreachable"
            })
            continue

        try:
            node_cfg = dict(node)
            node_cfg["connect_timeout"] = 1
            with psycopg2.connect(**node_cfg) as conn:
                with conn.cursor() as cur:
                    cur.execute("SELECT pg_is_in_recovery();")
                    is_in_recovery = cur.fetchone()[0]
                    replica_health.append({
                        "name": name,
                        "connected": True,
                        "is_in_recovery": is_in_recovery,
                        "endpoint": f"{host}:{port}",
                        "error": None
                    })
        except Exception as e:
            replica_health.append({
                "name": name,
                "connected": False,
                "is_in_recovery": None,
                "endpoint": f"{host}:{port}",
                "error": str(e)
            })
    return replica_health


def collect_all():
    """
    Convenience wrapper for periodic collector execution.
    Fast: if primary is unreachable, returns in <10ms immediately without blocking!
    """
    is_connected, msg = test_connection()
    if not is_connected:
        return {
            "connected": False,
            "query_stats": [],
            "table_scan_stats": [],
            "index_candidates": [],
            "replication_status": [],
            "standby_probes": [],
            "errors": {"primary": msg}
        }

    errors = {}
    query_stats, err = get_query_stats()
    if err:
        errors["query_stats"] = err

    table_scan_stats, err = get_table_scan_stats()
    if err:
        errors["table_scan_stats"] = err

    index_candidates, err = get_missing_index_candidates()
    if err:
        errors["index_candidates"] = err

    replication_status, err = get_replication_status()
    if err:
        errors["replication_status"] = err

    standby_probes = check_standby_replicas()

    return {
        "connected": True,
        "query_stats": query_stats,
        "table_scan_stats": table_scan_stats,
        "index_candidates": index_candidates,
        "replication_status": replication_status,
        "standby_probes": standby_probes,
        "errors": errors
    }
