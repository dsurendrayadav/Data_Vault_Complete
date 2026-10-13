"""
Comprehensive Automated Test Suite for DataVault Operations Hub.
Tests all ingestion collectors, analyzers, storage backends (PostgreSQL & SQLite),
FastAPI endpoints, and simulation dynamics.
"""
import os
import sys
import unittest

# Ensure project root is in sys.path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

import config
import storage
from ingestion import postgres_collector, mongo_collector
from analysis import query_analyzer, replication_monitor, shard_balance
import app as fastapi_app
from fastapi.testclient import TestClient


class TestDataVault(unittest.TestCase):

    def setUp(self):
        storage.init_db()

    def test_01_config_structure(self):
        """Verify configuration variables and separate PostgreSQL target."""
        self.assertIn("host", config.POSTGRES_CONFIG)
        self.assertEqual(config.POSTGRES_CONFIG["port"], 5432)
        self.assertEqual(config.POSTGRES_CONFIG["dbname"], "appdb")

        # Dedicated Application DB settings
        self.assertIn("host", config.DATAVAULT_PG_CONFIG)
        self.assertEqual(config.DATAVAULT_PG_CONFIG["port"], 5435)
        self.assertEqual(config.DATAVAULT_PG_CONFIG["dbname"], "datavault_db")
        self.assertEqual(config.DATAVAULT_PG_CONFIG["user"], "datavault_user")

    def test_02_storage_engine(self):
        """Verify storage engine snapshot creation, reading, and diagnostic info."""
        payload = {"metric_test": 123, "status": "ok"}
        storage.save_snapshot("test_metric", payload)

        latest = storage.get_latest_snapshot("test_metric")
        self.assertIsNotNone(latest)
        self.assertEqual(latest["payload"]["metric_test"], 123)

        history = storage.get_history("test_metric", limit=5)
        self.assertGreaterEqual(len(history), 1)

        info = storage.get_storage_info()
        self.assertIn(info["backend"], ("postgresql", "sqlite"))
        self.assertIn("total_snapshots", info)

    def test_03_query_analyzer(self):
        """Verify query ranking, clause parsing, and index recommendation generation."""
        mock_queries = [
            {
                "query": "SELECT * FROM transactions WHERE is_flagged = true AND created_at > NOW() - INTERVAL '30 days';",
                "calls": 100,
                "total_exec_time": 450000.0,
                "mean_exec_time": 4500.0,
                "rows": 500
            },
            {
                "query": "SELECT * FROM users WHERE email = $1 LIMIT 1;",
                "calls": 10000,
                "total_exec_time": 5000.0,
                "mean_exec_time": 0.5,
                "rows": 10000
            }
        ]

        ranked = query_analyzer.rank_slow_queries(mock_queries)
        self.assertEqual(len(ranked["by_total_time"]), 2)
        self.assertEqual(len(ranked["by_mean_time"]), 1)  # Only > 100ms
        self.assertEqual(ranked["by_mean_time"][0]["mean_exec_time"], 4500.0)

        # Test index recommendation
        mock_scans = [
            {
                "schemaname": "public",
                "table_name": "transactions",
                "seq_scan": 1500,
                "seq_tup_read": 15000000,
                "idx_scan": 50,
                "total_scans": 1550,
                "estimated_rows": 25000
            }
        ]
        mock_index_candidates = [
            {"table_name": "transactions", "indexed_columns": ["id"]}
        ]

        recs = query_analyzer.recommend_indexes(mock_scans, mock_index_candidates, mock_queries)
        self.assertEqual(len(recs), 1)
        self.assertEqual(recs[0]["table"], "transactions")
        self.assertEqual(recs[0]["confidence"], "high")
        self.assertIn("is_flagged", recs[0]["suggested_columns"])
        self.assertIn("created_at", recs[0]["suggested_columns"])
        self.assertIn("CREATE INDEX CONCURRENTLY", recs[0]["suggested_sql"])

    def test_04_replication_monitor(self):
        """Verify replication lag status thresholds."""
        mock_rep = [
            {"application_name": "pg-replica1", "replay_lag_seconds": 0.15, "state": "streaming"},
            {"application_name": "pg-replica2", "replay_lag_seconds": 12.0, "state": "streaming"},
            {"application_name": "pg-replica3", "replay_lag_seconds": 45.0, "state": "streaming"},
            {"application_name": "pg-replica4", "replay_lag_seconds": None, "state": "startup"},
        ]

        verdicts = replication_monitor.evaluate_replication_health(mock_rep)
        self.assertEqual(verdicts[0]["status"], "healthy")
        self.assertEqual(verdicts[1]["status"], "warning")
        self.assertEqual(verdicts[2]["status"], "critical")
        self.assertEqual(verdicts[3]["status"], "unknown")

    def test_05_shard_balance(self):
        """Verify MongoDB shard distribution balance calculation."""
        balanced_dist = {
            "doc_counts_by_shard": {"shard1rs": 10000, "shard2rs": 10050, "shard3rs": 9950}
        }
        b_res = shard_balance.evaluate_shard_balance(balanced_dist)
        self.assertTrue(b_res["is_balanced"])
        self.assertLess(b_res["max_deviation_pct"], 5.0)

        imbalanced_dist = {
            "doc_counts_by_shard": {"shard1rs": 35000, "shard2rs": 10000, "shard3rs": 5000}
        }
        ib_res = shard_balance.evaluate_shard_balance(imbalanced_dist)
        self.assertFalse(ib_res["is_balanced"])
        self.assertGreater(ib_res["max_deviation_pct"], 15.0)

    def test_06_fastapi_endpoints(self):
        """Verify all FastAPI routes respond properly."""
        client = TestClient(fastapi_app.app)

        # 1. Status
        res_status = client.get("/api/status")
        self.assertEqual(res_status.status_code, 200)
        data_status = res_status.json()
        self.assertIn("storage_info", data_status)
        self.assertIn("storage_backend", data_status)

        # 2. Metrics
        res_metrics = client.get("/api/metrics")
        self.assertEqual(res_metrics.status_code, 200)
        data_metrics = res_metrics.json()
        self.assertIn("postgres", data_metrics)
        self.assertIn("mongodb", data_metrics)
        # Verify fix for shard_distribution in mongodb analyzed payload
        self.assertIn("shard_distribution", data_metrics["mongodb"])

        # 3. History
        res_hist = client.get("/api/history?limit=10")
        self.assertEqual(res_hist.status_code, 200)

        # 4. Simulation actions
        res_seed = client.post("/api/seed?count=1000")
        self.assertEqual(res_seed.status_code, 200)

        res_bal = client.post("/api/balance")
        self.assertEqual(res_bal.status_code, 200)

        res_opt = client.post("/api/optimize")
        self.assertEqual(res_opt.status_code, 200)


if __name__ == "__main__":
    unittest.main()
