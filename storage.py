"""
DataVault Snapshot Storage Engine.

Supports a separate, dedicated PostgreSQL database for storing DataVault's own
application state, metric snapshots, and history. If the separate PostgreSQL
database is temporarily offline (e.g. in local simulation/demo mode), it seamlessly
falls back to local SQLite storage.
"""
import json
import socket
import sqlite3
import time
from contextlib import contextmanager
import psycopg2
import psycopg2.extras

from config import (
    DATAVAULT_PG_CONFIG,
    USE_POSTGRES_STORAGE,
    SQLITE_PATH,
    MAX_SNAPSHOTS
)

_POSTGRES_SCHEMA = """
CREATE TABLE IF NOT EXISTS datavault_snapshots (
    id SERIAL PRIMARY KEY,
    metric_type VARCHAR(64) NOT NULL,
    ts DOUBLE PRECISION NOT NULL,
    payload JSONB NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_dv_snapshots_type_ts ON datavault_snapshots(metric_type, ts);
"""

_SQLITE_SCHEMA = """
CREATE TABLE IF NOT EXISTS snapshots (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    metric_type TEXT NOT NULL,
    ts REAL NOT NULL,
    payload TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_snapshots_type_ts ON snapshots(metric_type, ts);
"""

_active_backend = None
_logged_fallback = False
_last_pg_check_time = 0
_last_pg_check_result = False


def _is_pg_reachable(host: str, port: int, timeout: float = 0.2) -> bool:
    """Fast socket check before attempting full psycopg2 connection."""
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.settimeout(timeout)
        target = "127.0.0.1" if host in ("localhost", "::1") else host
        res = s.connect_ex((target, int(port)))
        s.close()
        return res == 0
    except Exception:
        return False


def _check_postgres():
    """Test if the separate PostgreSQL application database is reachable with fast fail & caching."""
    global _last_pg_check_time, _last_pg_check_result
    if not USE_POSTGRES_STORAGE:
        return False

    now = time.time()
    if now - _last_pg_check_time < 5.0:
        return _last_pg_check_result

    host = DATAVAULT_PG_CONFIG.get("host", "localhost")
    port = int(DATAVAULT_PG_CONFIG.get("port", 5435))

    if not _is_pg_reachable(host, port, timeout=0.2):
        _last_pg_check_time = now
        _last_pg_check_result = False
        return False

    try:
        cfg = dict(DATAVAULT_PG_CONFIG)
        cfg["connect_timeout"] = 1
        conn = psycopg2.connect(**cfg)
        conn.close()
        _last_pg_check_result = True
    except Exception:
        _last_pg_check_result = False

    _last_pg_check_time = now
    return _last_pg_check_result


def get_active_backend():
    """Return 'postgresql' if separate PostgreSQL is active, else 'sqlite'."""
    global _active_backend, _logged_fallback
    if _check_postgres():
        _active_backend = "postgresql"
        return "postgresql"

    _active_backend = "sqlite"
    if not _logged_fallback:
        print(f"[storage] Notice: Dedicated PostgreSQL database ({DATAVAULT_PG_CONFIG['host']}:{DATAVAULT_PG_CONFIG['port']}/{DATAVAULT_PG_CONFIG['dbname']}) offline. Using SQLite fallback: {SQLITE_PATH}")
        _logged_fallback = True
    return "sqlite"


@contextmanager
def _get_pg_conn():
    cfg = dict(DATAVAULT_PG_CONFIG)
    cfg["connect_timeout"] = 1
    conn = psycopg2.connect(**cfg)
    try:
        yield conn
    finally:
        conn.close()


@contextmanager
def _get_sqlite_conn():
    conn = sqlite3.connect(SQLITE_PATH, timeout=5.0)
    try:
        yield conn
    finally:
        conn.close()


def init_db():
    """Initializes the database schema on PostgreSQL (preferred) or SQLite (fallback)."""
    backend = get_active_backend()
    if backend == "postgresql":
        try:
            with _get_pg_conn() as conn:
                with conn.cursor() as cur:
                    cur.execute(_POSTGRES_SCHEMA)
                conn.commit()
            return
        except Exception as e:
            print(f"[storage] PostgreSQL schema init failed ({e}), falling back to SQLite.")

    with _get_sqlite_conn() as conn:
        conn.executescript(_SQLITE_SCHEMA)
        conn.commit()


def save_snapshot(metric_type: str, payload: dict):
    """Saves a metric snapshot to the active storage engine and maintains retention."""
    ts = time.time()
    backend = get_active_backend()

    if backend == "postgresql":
        try:
            with _get_pg_conn() as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        "INSERT INTO datavault_snapshots (metric_type, ts, payload) VALUES (%s, %s, %s);",
                        (metric_type, ts, json.dumps(payload))
                    )
                    cur.execute("""
                        DELETE FROM datavault_snapshots
                        WHERE id IN (
                            SELECT id FROM datavault_snapshots
                            WHERE metric_type = %s
                            ORDER BY ts DESC
                            OFFSET %s
                        );
                    """, (metric_type, MAX_SNAPSHOTS))
                conn.commit()
            return
        except Exception as e:
            print(f"[storage] PostgreSQL insert failed ({e}), saving snapshot to SQLite fallback.")

    # SQLite fallback
    with _get_sqlite_conn() as conn:
        conn.execute(
            "INSERT INTO snapshots (metric_type, ts, payload) VALUES (?, ?, ?);",
            (metric_type, ts, json.dumps(payload))
        )
        conn.execute("""
            DELETE FROM snapshots
            WHERE id NOT IN (
                SELECT id FROM snapshots
                WHERE metric_type = ?
                ORDER BY ts DESC
                LIMIT ?
            ) AND metric_type = ?;
        """, (metric_type, MAX_SNAPSHOTS, metric_type))
        conn.commit()


def get_history(metric_type: str, limit: int = 50):
    """Retrieve historical metric snapshots in ascending chronological order."""
    backend = get_active_backend()

    if backend == "postgresql":
        try:
            with _get_pg_conn() as conn:
                with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                    cur.execute(
                        "SELECT ts, payload FROM datavault_snapshots WHERE metric_type = %s ORDER BY ts DESC LIMIT %s;",
                        (metric_type, limit)
                    )
                    rows = cur.fetchall()
            return [{"ts": r["ts"], "payload": r["payload"] if isinstance(r["payload"], dict) else json.loads(r["payload"])} for r in reversed(rows)]
        except Exception as e:
            print(f"[storage] PostgreSQL fetch failed ({e}), reading from SQLite fallback.")

    # SQLite fallback
    try:
        with _get_sqlite_conn() as conn:
            conn.row_factory = sqlite3.Row
            cur = conn.execute(
                "SELECT ts, payload FROM snapshots WHERE metric_type = ? ORDER BY ts DESC LIMIT ?;",
                (metric_type, limit)
            )
            rows = cur.fetchall()
        return [{"ts": r["ts"], "payload": json.loads(r["payload"])} for r in reversed(rows)]
    except Exception as e:
        print(f"[storage] SQLite fetch notice: {e}")
        return []


def get_latest_snapshot(metric_type: str):
    """Retrieve the single most recent metric snapshot for a given metric type."""
    history = get_history(metric_type, limit=1)
    if history:
        return history[-1]
    return None


def get_storage_info():
    """Return details about the currently active storage backend."""
    backend = get_active_backend()
    total_snapshots = 0

    if backend == "postgresql":
        try:
            with _get_pg_conn() as conn:
                with conn.cursor() as cur:
                    cur.execute("SELECT COUNT(*) FROM datavault_snapshots;")
                    total_snapshots = cur.fetchone()[0]
            return {
                "backend": "postgresql",
                "is_postgresql": True,
                "postgres_target": f"{DATAVAULT_PG_CONFIG['host']}:{DATAVAULT_PG_CONFIG['port']}/{DATAVAULT_PG_CONFIG['dbname']}",
                "sqlite_path": None,
                "total_snapshots": total_snapshots
            }
        except Exception:
            backend = "sqlite"

    try:
        with _get_sqlite_conn() as conn:
            cur = conn.execute("SELECT COUNT(*) FROM snapshots;")
            total_snapshots = cur.fetchone()[0]
    except Exception:
        total_snapshots = 0

    return {
        "backend": "sqlite",
        "is_postgresql": False,
        "postgres_target": None,
        "sqlite_path": SQLITE_PATH,
        "total_snapshots": total_snapshots
    }
