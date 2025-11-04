import argparse
import json
import os
import time
from pathlib import Path
from typing import Dict, Any, Optional

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.column import Column


# ---------- small helpers ----------

def _wide_in_dir(a: argparse.Namespace) -> str:
    """
    Resolve Silver-wide input directory for the given exp/trial/year/month.
    Preference: exp/ trial=.../year=.../month=... -> then exp/year=.../month=...
    """
    base = os.path.join(a.data_root, "silver_wide", "s3", f"exp={a.experiment_id}")
    if a.trial is not None:
        cand = os.path.join(base, f"trial={int(a.trial)}", f"year={a.year}", f"month={a.month}")
        if os.path.exists(cand):
            return cand
    cand = os.path.join(base, f"year={a.year}", f"month={a.month}")
    if os.path.exists(cand):
        return cand
    raise FileNotFoundError(f"No silver_wide input found under: {base}")

def _metrics_path(a: argparse.Namespace, step: str) -> str:
    """Build metrics JSON path under metrics/s3/exp=...[/trial=...]/year=/month=."""
    base = os.path.join(a.data_root, "metrics", "s3", f"exp={a.experiment_id}")
    if a.trial is not None:
        base = os.path.join(base, f"trial={int(a.trial)}")
    return os.path.join(base, f"year={a.year}", f"month={a.month}", f"{step}.json")

def _parse_args() -> argparse.Namespace:
    """CLI args for the job (kept compatible with the existing DAG)."""
    p = argparse.ArgumentParser()
    p.add_argument("--year", type=int, required=True)
    p.add_argument("--month", type=int, required=True)
    p.add_argument("--data-root", required=True)
    p.add_argument("--experiment-id", required=True)
    p.add_argument("--trial", type=int)
    return p.parse_args()


# ---------- main ----------

def main() -> None:
    a = _parse_args()
    in_dir = _wide_in_dir(a)

    # Local master is fine for this containerized, single-node benchmark job.
    spark = (
        SparkSession.builder
        .appName("dq_join")
        .config("spark.master", "local[*]")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    df = spark.read.parquet(in_dir)

    # DQ rules: NULL in ref-mapped columns => "unmapped"
    dq_fields: Dict[str, Column] = {
        "unmapped_vendor_name_pct":   F.col("vendor_name").isNull(),
        "unmapped_ratecode_desc_pct": F.col("ratecode_desc").isNull(),
        "unmapped_payment_desc_pct":  F.col("payment_desc").isNull(),
        "unmapped_pu_zone_name_pct":  F.col("pu_zone_name").isNull(),
        "unmapped_do_zone_name_pct":  F.col("do_zone_name").isNull(),
    }

    # Compute all metrics in a single aggregation:
    # avg(when(cond,1.0).otherwise(0.0)) * 100 gives percentage; count(*) gives rowcount.
    agg_exprs = [
        (F.avg(F.when(expr, F.lit(1.0)).otherwise(F.lit(0.0))) * F.lit(100.0)).alias(name)
        for name, expr in dq_fields.items()
    ]
    agg_exprs.append(F.count(F.lit(1)).alias("__rows"))

    row = df.agg(*agg_exprs).first()
    rows: int = int(row["__rows"]) if row is not None and row["__rows"] is not None else 0

    # On empty input avg(...) returns None — normalize to 0.0
    metrics_pct: Dict[str, float] = {
        name: round(float((row[name] or 0.0)), 6) for name in dq_fields.keys()
    } if row is not None else {name: 0.0 for name in dq_fields.keys()}

    out: Dict[str, Any] = {
        "step": "dq_join",
        "year": a.year,
        "month": a.month,
        "metrics_pct": metrics_pct,
        "rows": rows,
        "computed_at": int(time.time()),
    }

    mp = _metrics_path(a, "dq_join")
    Path(os.path.dirname(mp)).mkdir(parents=True, exist_ok=True)
    with open(mp, "w", encoding="utf-8") as f:
        json.dump(out, f, ensure_ascii=False, indent=2)

    # Print to stdout for quick inspection in task logs
    print(json.dumps(out, ensure_ascii=False))

    spark.stop()


if __name__ == "__main__":
    main()
