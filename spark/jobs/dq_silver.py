from __future__ import annotations

import os
import json
import argparse
from pathlib import Path
from typing import Dict, Any

from pyspark.sql import SparkSession, functions as F


# ---------- utils ----------

def metrics_path(data_root: str, experiment_id: str, trial: int, y: int, m: int, step: str) -> str:
    """Build metrics JSON path under metrics/s3/exp=.../trial=.../year=.../month=..."""
    base = os.path.join(
        data_root, "metrics", "s3", f"exp={experiment_id}", f"trial={trial}",
        f"year={y}", f"month={m}"
    )
    Path(base).mkdir(parents=True, exist_ok=True)
    return os.path.join(base, f"{step}.json")


def silver_dir(data_root: str, experiment_id: str, trial: int, y: int, m: int) -> str:
    """Locate Silver facts directory for a given exp/trial/period."""
    return os.path.join(
        data_root, "silver", "s3", f"exp={experiment_id}", f"trial={trial}",
        f"year={y}", f"month={m}"
    )


def pct(n: int, d: int) -> float:
    """Safe percent with empty-denominator guard."""
    if not d:
        return 0.0
    return float(n) * 100.0 / float(d)


# ---------- main ----------

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--data-root", required=True)
    ap.add_argument("--experiment-id", required=True)
    ap.add_argument("--trial", type=int, required=True)
    ap.add_argument("--year", type=int, required=True)
    ap.add_argument("--month", type=int, required=True)
    args = ap.parse_args()

    spark = (
        SparkSession.builder
        .appName("dq_silver")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    src = silver_dir(args.data_root, args.experiment_id, args.trial, args.year, args.month)
    df = spark.read.parquet(src)

    # ---- Core DQ rules as boolean expressions (NULLs, non-positive values, etc.)
    # We compute all percentages in a single aggregation: avg(when(cond,1).otherwise(0)) * 100
    rules: Dict[str, F.Column] = {
        "null_pickup_ts_pct":     F.col("pickup_ts").isNull(),
        "null_dropoff_ts_pct":    F.col("dropoff_ts").isNull(),
        "null_pu_zone_id_pct":    F.col("pu_zone_id").isNull(),
        "null_do_zone_id_pct":    F.col("do_zone_id").isNull(),
        "null_payment_type_pct":  F.col("payment_type").isNull(),
        "zero_distance_pct":      F.col("trip_distance") <= F.lit(0),
        # "non-positive duration" (<=) → counted as zero_duration; strict (<) also reported separately
        "zero_duration_pct":      F.col("dropoff_ts") <= F.col("pickup_ts"),
        "drop_lt_pickup_pct":     F.col("dropoff_ts") <  F.col("pickup_ts"),
        "negative_total_pct":     F.col("total_amount") < F.lit(0),
    }

    agg_exprs = [
        (F.avg(F.when(expr, F.lit(1.0)).otherwise(F.lit(0.0))) * F.lit(100.0)).alias(name)
        for name, expr in rules.items()
    ]
    agg_exprs.append(F.count(F.lit(1)).alias("__rows_total"))

    agg_row = df.agg(*agg_exprs).first()
    rows_total: int = int(agg_row["__rows_total"]) if agg_row and agg_row["__rows_total"] is not None else 0

    # Normalize None → 0.0 on empty input for stable schema
    metrics_pct: Dict[str, float] = {
        name: round(float(agg_row[name] or 0.0), 6) if agg_row else 0.0
        for name in rules.keys()
    }

    # ---- Duplicate detection on a natural-ish key set (same logic as before)
    # sum(count - 1) over duplicate groups => number of "extra" rows
    duplicate_key_pct = 0.0
    key_candidates = ["vendor_id", "pickup_ts", "pu_zone_id", "do_zone_id", "total_amount", "row_hash"]
    present_keys = [c for c in key_candidates if c in df.columns]
    if rows_total and present_keys:
        dup_groups = df.groupBy(*present_keys).count().where(F.col("count") > 1)
        extra = dup_groups.agg(F.sum(F.col("count") - F.lit(1)).alias("extra")).collect()[0]["extra"]
        extra = int(extra or 0)
        duplicate_key_pct = round(pct(extra, rows_total), 6)

    # Compose output payload (unchanged schema)
    out: Dict[str, Any] = {
        "step": "dq_silver",
        "year": args.year,
        "month": args.month,
        "rows_total": rows_total,
        "metrics_pct": {
            **metrics_pct,
            "duplicate_key_pct": duplicate_key_pct,
        },
    }

    out_path = metrics_path(args.data_root, args.experiment_id, args.trial, args.year, args.month, "dq_silver")
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(out, f, ensure_ascii=False, indent=2)

    spark.stop()


if __name__ == "__main__":
    main()
