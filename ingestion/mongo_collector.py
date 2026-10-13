"""
Collects operational metrics from the MongoDB sharded cluster via mongos:
  - Overall server status (connections, memory, opcounters)
  - Per-shard document and chunk distribution for the tracked collection
  - Health and list of shards as reported by the cluster router
"""
import socket
from urllib.parse import urlparse
from pymongo import MongoClient

from config import MONGO_URI, MONGO_DB, MONGO_COLLECTION


def _is_mongo_reachable(uri: str = MONGO_URI, timeout: float = 0.3) -> bool:
    """Fast socket probe to check if MongoDB port is open."""
    try:
        parsed = urlparse(uri)
        host = parsed.hostname or "127.0.0.1"
        port = parsed.port or 27017
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.settimeout(timeout)
        res = s.connect_ex((host, port))
        s.close()
        return res == 0
    except Exception:
        return False


def _client():
    return MongoClient(
        MONGO_URI,
        serverSelectionTimeoutMS=800,
        connectTimeoutMS=800,
        socketTimeoutMS=1000
    )


def test_connection():
    """Quick check if MongoDB is reachable."""
    if not _is_mongo_reachable():
        return False, "MongoDB port is closed or unreachable."
    try:
        with _client() as client:
            client.admin.command("ping")
            return True, "MongoDB reachable"
    except Exception as e:
        return False, str(e)


def get_server_status():
    try:
        with _client() as client:
            status = client.admin.command("serverStatus")
            return {
                "connections_current": status.get("connections", {}).get("current"),
                "connections_available": status.get("connections", {}).get("available"),
                "opcounters": status.get("opcounters", {}),
                "mem_resident_mb": status.get("mem", {}).get("resident"),
                "mem_virtual_mb": status.get("mem", {}).get("virtual"),
                "uptime_seconds": status.get("uptime"),
            }, None
    except Exception as e:
        return {}, f"Server status notice: {str(e)}"


def get_shard_list():
    try:
        with _client() as client:
            result = client.admin.command("listShards")
            return [{"id": s["_id"], "host": s["host"]} for s in result.get("shards", [])], None
    except Exception as e:
        return [], f"List shards notice: {str(e)}"


def get_shard_distribution():
    """
    Mirrors what `db.collection.getShardDistribution()` shows in the mongo shell.
    Supports modern MongoDB (v5/v6/v7 UUID chunks) with backward compatibility fallbacks.
    """
    try:
        with _client() as client:
            db = client[MONGO_DB]
            config_db = client["config"]
            ns = f"{MONGO_DB}.{MONGO_COLLECTION}"

            # 1. Chunk distribution from config database
            chunk_counts_by_shard = {}
            try:
                coll_doc = config_db["collections"].find_one({"_id": ns})
                chunks = []
                if coll_doc and "uuid" in coll_doc:
                    chunks = list(config_db["chunks"].find({"uuid": coll_doc["uuid"]}))
                if not chunks:
                    chunks = list(config_db["chunks"].find({"ns": ns}))

                for c in chunks:
                    shard = c.get("shard")
                    if shard:
                        chunk_counts_by_shard[shard] = chunk_counts_by_shard.get(shard, 0) + 1
            except Exception:
                chunk_counts_by_shard = {}

            # 2. Document count per shard
            doc_counts_by_shard = {}
            try:
                pipeline = [{"$shardedDataDistribution": {}}, {"$match": {"ns": ns}}]
                dist_results = list(client.admin.aggregate(pipeline))
                if dist_results:
                    for shard_info in dist_results[0].get("shards", []):
                        shard_name = shard_info.get("shardName")
                        doc_counts_by_shard[shard_name] = shard_info.get("numOwnedDocuments", 0)
                else:
                    raise ValueError("No aggregate sharding distribution found")
            except Exception:
                try:
                    pipeline = [{"$collStats": {"storageStats": {}}}]
                    for stat in db[MONGO_COLLECTION].aggregate(pipeline):
                        shard = stat.get("shard", "unknown")
                        storage_stats = stat.get("storageStats", {})
                        doc_counts_by_shard[shard] = storage_stats.get("count", 0)
                except Exception:
                    try:
                        total_docs = db[MONGO_COLLECTION].count_documents({})
                        doc_counts_by_shard["standalone"] = total_docs
                    except Exception as e:
                        return {}, f"Could not count documents: {str(e)}"

            total_docs = sum(doc_counts_by_shard.values())
            return {
                "ns": ns,
                "total_docs": total_docs,
                "doc_counts_by_shard": doc_counts_by_shard,
                "chunk_counts_by_shard": chunk_counts_by_shard
            }, None
    except Exception as e:
        return {}, f"Failed to retrieve shard distribution: {str(e)}"


def collect_all():
    """
    Convenience wrapper for periodic collector execution.
    Fast: if MongoDB port is closed, returns immediately without blocking.
    """
    if not _is_mongo_reachable():
        return {
            "connected": False,
            "server_status": {},
            "shards": [],
            "shard_distribution": {},
            "errors": {"mongo": "MongoDB port is closed or unreachable."}
        }

    errors = {}
    server_status, err = get_server_status()
    if err:
        errors["server_status"] = err

    shards, err = get_shard_list()
    if err:
        errors["shards"] = err

    shard_dist, err = get_shard_distribution()
    if err:
        errors["shard_distribution"] = err

    connected = len(server_status) > 0 or len(shards) > 0 or len(shard_dist) > 0

    return {
        "connected": connected,
        "server_status": server_status,
        "shards": shards,
        "shard_distribution": shard_dist,
        "errors": errors
    }
