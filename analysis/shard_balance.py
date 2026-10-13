"""
Turns raw Mongo shard distribution data into a balance verdict:
how far each shard deviates from a perfectly even split.
"""
from config import SHARD_IMBALANCE_PCT_THRESHOLD


def evaluate_shard_balance(shard_distribution: dict):
    if not isinstance(shard_distribution, dict):
        return {
            "shards": [],
            "is_balanced": None,
            "note": "No document count data available yet — has data been seeded into the sharded collection?",
        }

    doc_counts_raw = shard_distribution.get("doc_counts_by_shard")
    if not isinstance(doc_counts_raw, dict):
        return {
            "shards": [],
            "is_balanced": None,
            "note": "No document count data available yet — has data been seeded into the sharded collection?",
        }

    doc_counts = {
        k: v for k, v in doc_counts_raw.items()
        if k != "error" and isinstance(v, (int, float))
    }

    if not doc_counts:
        # Check if there is an error logged inside the doc_counts_by_shard
        err_msg = doc_counts_raw.get("error")
        note = f"No document count data available: {err_msg}" if err_msg else "No document count data available yet — has data been seeded into the sharded collection?"
        return {
            "shards": [],
            "is_balanced": None,
            "note": note,
        }

    total_docs = sum(doc_counts.values())
    shard_count = len(doc_counts)
    expected_per_shard = total_docs / shard_count if shard_count else 0

    shard_results = []
    max_deviation_pct = 0.0
    for shard, count in doc_counts.items():
        deviation_pct = (
            abs(count - expected_per_shard) / expected_per_shard * 100
            if expected_per_shard else 0
        )
        max_deviation_pct = max(max_deviation_pct, deviation_pct)
        shard_results.append({
            "shard": shard,
            "doc_count": count,
            "pct_of_total": round(count / total_docs * 100, 1) if total_docs else 0,
            "deviation_from_even_pct": round(deviation_pct, 1),
        })

    shard_results.sort(key=lambda s: s["doc_count"], reverse=True)

    return {
        "shards": shard_results,
        "total_docs": total_docs,
        "expected_per_shard": round(expected_per_shard),
        "max_deviation_pct": round(max_deviation_pct, 1),
        "is_balanced": max_deviation_pct <= SHARD_IMBALANCE_PCT_THRESHOLD,
        "note": (
            "Distribution looks even across shards."
            if max_deviation_pct <= SHARD_IMBALANCE_PCT_THRESHOLD
            else f"Largest shard deviates {max_deviation_pct:.1f}% from an even split — "
                 f"consider reviewing the shard key choice or letting the balancer catch up."
        ),
    }
