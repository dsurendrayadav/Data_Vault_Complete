"""
Turns raw Postgres stats into actionable output:
  - Ranked slow queries (by total time and mean execution time)
  - Intelligent index recommendations based on sequential scan patterns and parsed WHERE/JOIN/ORDER BY columns
"""
import re

from config import SLOW_QUERY_MS_THRESHOLD, SEQ_SCAN_RATIO_THRESHOLD, MIN_TABLE_ROWS_FOR_INDEX_ADVICE

# SQL Keywords that should never be treated as column names
_SQL_KEYWORDS = {
    "select", "from", "where", "and", "or", "not", "null", "true", "false",
    "order", "by", "group", "having", "limit", "offset", "join", "left",
    "right", "inner", "outer", "cross", "on", "as", "is", "in", "like",
    "ilike", "between", "now", "interval", "desc", "asc", "returning"
}

_FILTER_COL_RE = re.compile(
    r"\b(?:WHERE|AND|OR)\s+(?:[a-zA-Z_][a-zA-Z0-9_]*\.)?([a-zA-Z_][a-zA-Z0-9_]*)\s*(?:=|<|>|<=|>=|!=|<>|\bIN\b|\bLIKE\b|\bILIKE\b|\bIS\b|\bBETWEEN\b)",
    re.IGNORECASE
)

_JOIN_COL_RE = re.compile(
    r"\bON\s+(?:[a-zA-Z_][a-zA-Z0-9_]*\.)?([a-zA-Z_][a-zA-Z0-9_]*)\s*=\s*(?:[a-zA-Z_][a-zA-Z0-9_]*\.)?([a-zA-Z_][a-zA-Z0-9_]*)",
    re.IGNORECASE
)

_ORDER_BY_COL_RE = re.compile(
    r"\bORDER\s+BY\s+(?:[a-zA-Z_][a-zA-Z0-9_]*\.)?([a-zA-Z_][a-zA-Z0-9_]*)",
    re.IGNORECASE
)

_TABLES_RE = re.compile(
    r"\b(?:FROM|JOIN|UPDATE|INTO)\s+([a-zA-Z_][a-zA-Z0-9_]*)",
    re.IGNORECASE
)


def rank_slow_queries(query_stats: list, limit: int = 15):
    """
    Rank queries by total execution time and highest mean execution time.
    """
    if not isinstance(query_stats, list):
        return {
            "by_total_time": [],
            "by_mean_time": [],
            "threshold_ms": SLOW_QUERY_MS_THRESHOLD,
            "error": "Query stats not available"
        }

    valid_stats = [q for q in query_stats if isinstance(q, dict)]

    slow = [q for q in valid_stats if q.get("mean_exec_time", 0) >= SLOW_QUERY_MS_THRESHOLD]
    slow_by_total = sorted(valid_stats, key=lambda q: q.get("total_exec_time", 0), reverse=True)[:limit]
    slow_by_mean = sorted(slow, key=lambda q: q.get("mean_exec_time", 0), reverse=True)[:limit]

    return {
        "by_total_time": slow_by_total,
        "by_mean_time": slow_by_mean,
        "threshold_ms": SLOW_QUERY_MS_THRESHOLD,
    }


def _extract_candidate_columns(query_text: str):
    cols = set()
    if not query_text or not isinstance(query_text, str):
        return cols

    # 1. Filter clauses (WHERE, AND, OR)
    for m in _FILTER_COL_RE.finditer(query_text):
        c = m.group(1).lower()
        if c not in _SQL_KEYWORDS:
            cols.add(c)

    # 2. Join clauses
    for m in _JOIN_COL_RE.finditer(query_text):
        for idx in (1, 2):
            c = m.group(idx).lower()
            if c not in _SQL_KEYWORDS:
                cols.add(c)

    # 3. Order By columns
    for m in _ORDER_BY_COL_RE.finditer(query_text):
        c = m.group(1).lower()
        if c not in _SQL_KEYWORDS:
            cols.add(c)

    return cols


def _extract_tables(query_text: str):
    tables = set()
    if not query_text or not isinstance(query_text, str):
        return tables
    for m in _TABLES_RE.finditer(query_text):
        t = m.group(1).lower()
        if t not in _SQL_KEYWORDS:
            tables.add(t)
    return tables


def recommend_indexes(table_scan_stats: list, index_candidates: list, query_stats: list):
    """
    Combine three signals:
      1. Tables with high sequential-to-index scan ratio (scan-heavy tables)
      2. Table row count large enough that sequential scans cause significant I/O
      3. Candidate columns extracted from tracked slow queries that are not yet indexed
    """
    if not isinstance(table_scan_stats, list) or not isinstance(index_candidates, list) or not isinstance(query_stats, list):
        return []

    valid_candidates = [r for r in index_candidates if isinstance(r, dict)]
    valid_scans = [r for r in table_scan_stats if isinstance(r, dict)]
    valid_queries = [r for r in query_stats if isinstance(r, dict)]

    indexed_cols_by_table = {
        row["table_name"]: set(c.lower() for c in (row.get("indexed_columns") or []))
        for row in valid_candidates if "table_name" in row
    }

    # Map table -> candidate columns pulled from slow query text
    cols_from_queries = {}
    for q in valid_queries:
        query_text = q.get("query")
        if not query_text:
            continue
        tables = _extract_tables(query_text)
        cols = _extract_candidate_columns(query_text)
        if cols:
            for table in tables:
                cols_from_queries.setdefault(table, set()).update(cols)

    recommendations = []
    for row in valid_scans:
        table = row.get("table_name")
        if not table:
            continue
        seq_scan = row.get("seq_scan") or 0
        idx_scan = row.get("idx_scan") or 0
        total = seq_scan + idx_scan
        est_rows = row.get("estimated_rows") or 0

        if total == 0 or est_rows < MIN_TABLE_ROWS_FOR_INDEX_ADVICE:
            continue

        seq_ratio = seq_scan / total
        if seq_ratio < SEQ_SCAN_RATIO_THRESHOLD:
            continue

        already_indexed = indexed_cols_by_table.get(table, set())
        candidate_cols = cols_from_queries.get(table, set()) - already_indexed
        sorted_cols = sorted(candidate_cols) if candidate_cols else None

        suggested_sql = None
        if sorted_cols:
            col_joined = "_".join(sorted_cols)
            suggested_sql = f"CREATE INDEX CONCURRENTLY idx_{table}_{col_joined} ON {table} ({', '.join(sorted_cols)});"

        recommendations.append({
            "table": table,
            "seq_scan_ratio": round(seq_ratio, 3),
            "seq_scan_count": seq_scan,
            "idx_scan_count": idx_scan,
            "estimated_rows": est_rows,
            "suggested_columns": sorted_cols,
            "suggested_sql": suggested_sql,
            "confidence": "high" if sorted_cols else "low",
            "note": (
                f"Table '{table}' is scanned sequentially {seq_ratio:.0%} of the time across "
                f"~{est_rows:,} rows."
                + (f" Columns {sorted_cols} appear in query filters/joins but lack an index."
                   if sorted_cols else
                   " Sequential scans are predominant; no unindexed WHERE/JOIN filters found in recent statements.")
            ),
        })

    recommendations.sort(key=lambda r: (r["confidence"] == "low", -r["seq_scan_count"]))
    return recommendations
