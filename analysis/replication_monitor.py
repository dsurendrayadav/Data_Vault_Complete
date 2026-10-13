"""
Turns raw pg_stat_replication rows into a simple health verdict per replica.
"""

# Lag thresholds in seconds
LAG_WARN_SECONDS = 5.0
LAG_CRITICAL_SECONDS = 30.0


def evaluate_replication_health(replication_status: list):
    if not isinstance(replication_status, list):
        return []

    results = []
    for row in replication_status:
        if not isinstance(row, dict):
            continue
        lag = row.get("replay_lag_seconds")
        if lag is None:
            status = "unknown"
        elif lag >= LAG_CRITICAL_SECONDS:
            status = "critical"
        elif lag >= LAG_WARN_SECONDS:
            status = "warning"
        else:
            status = "healthy"

        results.append({
            "replica": row.get("application_name") or row.get("client_addr") or "unknown_replica",
            "state": row.get("state") or "unknown",
            "replay_lag_seconds": lag,
            "write_lag_seconds": row.get("write_lag_seconds"),
            "status": status,
        })
    return results
