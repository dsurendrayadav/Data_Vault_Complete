"""
DataVault Telemetry Collector Daemon.
Runs continuously, polling both databases on a configured interval and storing snapshots
in the dedicated PostgreSQL application database (with SQLite fallback) so the dashboard
can render real-time and historical trend lines.

Usage:
    python collector_daemon.py --interval 15
"""
import argparse
import time
import traceback

import storage
from ingestion import postgres_collector, mongo_collector


def run_once():
    try:
        pg_data = postgres_collector.collect_all()
        storage.save_snapshot("postgres", pg_data)
    except Exception as e:
        print(f"[postgres_collector] error: {e}")
        traceback.print_exc()

    try:
        mongo_data = mongo_collector.collect_all()
        storage.save_snapshot("mongo", mongo_data)
    except Exception as e:
        print(f"[mongo_collector] error: {e}")
        traceback.print_exc()


def main():
    parser = argparse.ArgumentParser(description="DataVault Metric Collector Daemon")
    parser.add_argument("--interval", type=float, default=15.0, help="Seconds between collections")
    parser.add_argument("--once", action="store_true", help="Run a single collection and exit")
    args = parser.parse_args()

    storage.init_db()

    if args.once:
        run_once()
        print("Single collection pass completed.")
        return

    print(f"Collector daemon started. Polling every {args.interval}s. Ctrl+C to stop.")
    while True:
        start = time.time()
        run_once()
        elapsed = time.time() - start
        sleep_for = max(0.0, args.interval - elapsed)
        time.sleep(sleep_for)


if __name__ == "__main__":
    main()
