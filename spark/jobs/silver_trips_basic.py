from __future__ import annotations

import argparse
import json
import os
import time
import glob
import io
from typing import Tuple, Dict, Any, List

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, lit, upper, when, coalesce as coalesce_fn,
    to_timestamp, date_format, year as year_, month as month_,
    sha2, concat_ws
)
from pyspark import StorageLevel

# ===== Canonical column set we produce in silver =====
CANON_COLS = [
    "vendor_id", "pickup_ts", "dropoff_ts", "passenger_count", "trip_distance",
    "ratecode_id", "store_and_fwd_flag", "pu_zone_id", "do_zone_id",
    "payment_type", "fare_amount", "extra", "mta_tax", "tip_amount",
    "tolls_amount", "improvement_surcharge", "total_amount", "congestion_surcharge",
    "pickup_date_id", "dropoff_date_id", "year", "month",
    "row_hash",
    "is_zero_distance", "is_zero_duration", "is_negative_total", "is_passenger_missing",
]

# Column name candidates (ingest from many schemas → normalize to CANON_COLS)
CANDIDATES: Dict[str, List[str]] = {
    "vendor_id": ["vendor_id", "vendorid", "VendorID"],
    "pickup_ts": ["pickup_ts", "tpep_pickup_datetime", "pickup_datetime", "pickup_at"],
    "dropoff_ts": ["dropoff_ts", "tpep_dropoff_datetime", "dropoff_datetime", "dropoff_at"],
    "passenger_count": ["passenger_count", "passenger_cnt"],
    "trip_distance": ["trip_distance", "trip_miles", "trip_distance_mi"],
    "ratecode_id": ["ratecode_id", "ratecodeid", "RatecodeID"],
    "store_and_fwd_flag": ["store_and_fwd_flag", "store_and_fwd"],
    "pu_zone_id": ["pu_zone_id", "pulocationid", "PULocationID", "pickup_location_id"],
    "do_zone_id": ["do_zone_id", "dolocationid", "DOLocationID", "dropoff_location_id"],
    "payment_type": ["payment_type", "paymenttype"],
    "fare_amount": ["fare_amount"],
    "extra": ["extra"],
    "mta_tax": ["mta_tax"],
    "tip_amount": ["tip_amount"],
    "tolls_amount": ["tolls_amount"],
    "improvement_surcharge": ["improvement_surcharge"],
    "total_amount": ["total_amount"],
    "congestion_surcharge": ["congestion_surcharge"],
    "year": ["year"],
    "month": ["month"],
    "row_hash": ["row_hash", "hash"],
}

INTS    = {"vendor_id", "passenger_count", "ratecode_id", "pu_zone_id", "do_zone_id", "payment_type", "year", "month"}
DOUBLES = {"trip_distance", "fare_amount", "extra", "mta_tax", "tip_amount", "tolls_amount",
           "improvement_surcharge", "total_amount", "congestion_surcharge"}
TIMESTAMPS = {"pickup_ts", "dropoff_ts"}

# ---------- small utils ----------
def _dir_size_mb(path: str) -> float:
    """Compute folder size in MB (best-effort)."""
    total = 0
    for root, _, files in os.walk(path):
        for name in files:
            try:
                total += os.path.getsize(os.path.join(root, name))
            except OSError:
                pass
    return round(total / (1024 * 1024), 3)

def cleanup_dir(path: str) -> None:
    """Delete all contents under path, keep root directory."""
    if not os.path.exists(path):
        return
    for root, dirs, files in os.walk(path, topdown=False):
        for f in files:
            try:
                os.remove(os.path.join(root, f))
            except FileNotFoundError:
                pass
        for d in dirs:
            try:
                os.rmdir(os.path.join(root, d))
            except OSError:
                pass

def bronze_in_dir(a) -> str:
    """Resolve bronze input dir, prefer trial/… if provided."""
    base = os.path.join(a.data_root, "bronze", "s3", f"exp={a.experiment_id}")
    with_trial = (os.path.join(base, f"trial={int(a.trial)}", f"year={a.year}", f"month={a.month}")
                  if a.trial is not None else None)
    no_trial = os.path.join(base, f"year={a.year}", f"month={a.month}")
    for p in (with_trial, no_trial):
        if p and os.path.exists(p):
            return p
    raise FileNotFoundError(f"No bronze input found (tried: {with_trial}, {no_trial})")

def first_col(df, names: List[str]):
    """Pick first existing column by name from candidates."""
    for n in names:
        if n in df.columns:
            return col(n)
    return None

def _eventlog_stats(eventlog_dir: str, app_id: str) -> Dict[str, Any]:
    """Parse Spark event log for shuffle/gc/exec-time/task/stage metrics."""
    path = os.path.join(eventlog_dir, app_id)
    if not os.path.exists(path):
        cands = glob.glob(os.path.join(eventlog_dir, f"{app_id}*"))
        path = cands[0] if cands else None
    if not path or not os.path.exists(path):
        return {k: 0 for k in [
            "shuffle_read_mb","shuffle_write_mb","tasks_total","stages",
            "disk_spill_mb","memory_spill_mb","gc_time_ms","executor_run_time_ms","skew_factor"
        ]}
    read_b = write_b = mem_spill = disk_spill = gc_ms = run_ms = 0
    tasks = 0
    run_times: List[int] = []
    stages_seen = set()
    with io.open(path, "r", encoding="utf-8") as f:
        for line in f:
            try:
                ev = json.loads(line)
            except Exception:
                continue
            et = ev.get("Event")
            if et == "SparkListenerTaskEnd":
                tasks += 1
                tm = ev.get("Task Metrics") or {}
                srm = tm.get("Shuffle Read Metrics") or {}
                swm = tm.get("Shuffle Write Metrics") or {}
                read_b += int(srm.get("Total Bytes Read") or (srm.get("Remote Bytes Read", 0) + srm.get("Local Bytes Read", 0)))
                wb = swm.get("Shuffle Bytes Written")
                if wb is None:
                    wb = swm.get("Bytes Written", 0)
                write_b += int(wb)
                mem_spill += int(tm.get("Memory Bytes Spilled", 0))
                disk_spill += int(tm.get("Disk Bytes Spilled", 0))
                gc_ms += int(tm.get("JVM GC Time", 0))
                r = int(tm.get("Executor Run Time", 0)); run_ms += r
                if r > 0:
                    run_times.append(r)
            elif et == "SparkListenerStageCompleted":
                si = ev.get("Stage Info") or {}
                sid = si.get("Stage ID")
                if sid is not None:
                    stages_seen.add(sid)
    mb = 1024 * 1024
    skew = None
    if run_times:
        mx = max(run_times); mean = sum(run_times)/len(run_times)
        if mean > 0:
            skew = round(mx/mean, 6)
    return {
        "shuffle_read_mb": round(read_b/mb,3),
        "shuffle_write_mb": round(write_b/mb,3),
        "tasks_total": tasks, "stages": len(stages_seen),
        "disk_spill_mb": round(disk_spill/mb,3), "memory_spill_mb": round(mem_spill/mb,3),
        "gc_time_ms": gc_ms, "executor_run_time_ms": run_ms, "skew_factor": skew
    }

# ---------- CLI ----------
def argp():
    p = argparse.ArgumentParser()
    p.add_argument("--year", type=int, required=True)
    p.add_argument("--month", type=int, required=True)
    p.add_argument("--data-root", required=True)
    p.add_argument("--experiment-id", required=True)
    p.add_argument("--trial", type=int)
    # tuning
    p.add_argument("--parquet-codec", default="snappy")
    p.add_argument("--shuffle-partitions", type=int, default=64)
    p.add_argument("--max-partition-bytes", type=int, default=128 * 1024 * 1024)
    p.add_argument("--aqe", action="store_true")
    p.add_argument("--target-files-per-month", type=int, default=1)
    return p.parse_args()

# ---------- main ----------
def main():
    a = argp()

    # ====== PATHS ======
    in_dir = bronze_in_dir(a)
    print(f"[silver_prep] reading bronze from: {in_dir}")

    silver_base = os.path.join(a.data_root, "silver", "s3", f"exp={a.experiment_id}")
    if a.trial is not None:
        silver_base = os.path.join(silver_base, f"trial={a.trial}")
    out_dir = os.path.join(silver_base, f"year={a.year}", f"month={a.month}")

    # Event log for shuffle metrics
    eventlog_dir = os.path.join(a.data_root, "_spark_eventlog")
    os.makedirs(eventlog_dir, exist_ok=True)

    # ====== SPARK ======
    spark = (
        SparkSession.builder
        .appName("silver_trips_basic")
        .config("spark.master", "local[*]")
        .config("spark.sql.shuffle.partitions", a.shuffle_partitions)
        .config("spark.sql.files.maxPartitionBytes", a.max_partition_bytes)
        .config("spark.sql.adaptive.enabled", "true" if a.aqe else "false")
        .config("spark.sql.parquet.compression.codec", a.parquet_codec)
        .config("spark.eventLog.enabled", "true")
        .config("spark.eventLog.dir", f"file://{eventlog_dir}")
        .config("spark.eventLog.compress", "false")
        .getOrCreate()
    )
    app_id = spark.sparkContext.applicationId
    spark.sparkContext.setLogLevel("WARN")

    t0 = time.time()

    # ====== READ ======
    df = spark.read.parquet(in_dir).persist(StorageLevel.MEMORY_AND_DISK)
    partitions_in = df.rdd.getNumPartitions()
    rows_in = df.count()  # materialize input

    # Hint: when writing many output files, pre-increase parallelism a bit
    if a.target_files_per_month and a.target_files_per_month > 1:
        df = df.repartition(a.target_files_per_month)

    # ====== TRANSFORM ======
    out = df
    # 1) Normalize columns to canonical names
    for tgt, cand in CANDIDATES.items():
        src = first_col(out, cand)
        out = out.withColumn(tgt, src if src is not None else lit(None))

    # 2) Casts (stable types downstream)
    for c in INTS:
        out = out.withColumn(c, out[c].cast("int"))
    for c in DOUBLES:
        out = out.withColumn(c, out[c].cast("double"))
    for c in TIMESTAMPS:
        out = out.withColumn(c, to_timestamp(out[c]))

    # 3) Fill defaults for enums / flags (domain-friendly)
    out = (
        out
        .withColumn("vendor_id",    coalesce_fn(col("vendor_id"), lit(99)))
        .withColumn("ratecode_id",  coalesce_fn(col("ratecode_id"), lit(99)))
        .withColumn("payment_type", coalesce_fn(col("payment_type"), lit(5)))
        .withColumn(
            "store_and_fwd_flag",
            when(upper(col("store_and_fwd_flag")).isin("Y", "N"), upper(col("store_and_fwd_flag"))).otherwise(lit("U"))
        )
        .withColumn("passenger_count", coalesce_fn(col("passenger_count"), lit(0)))
    )

    # 4) Derived date fields
    out = (
        out
        .withColumn("pickup_date_id",  date_format(col("pickup_ts"),  "yyyyMMdd").cast("int"))
        .withColumn("dropoff_date_id", date_format(col("dropoff_ts"), "yyyyMMdd").cast("int"))
        .withColumn("year",  coalesce_fn(col("year"),  year_(col("pickup_ts"))).cast("int"))
        .withColumn("month", coalesce_fn(col("month"), month_(col("pickup_ts"))).cast("int"))
    )

    # 5) Simple DQ flags (helpful for downstream rules)
    out = (
        out
        .withColumn("is_zero_distance",  (coalesce_fn(col("trip_distance"), lit(0.0)) <= 0.0))
        .withColumn("is_zero_duration",  (col("dropoff_ts").cast("long") - col("pickup_ts").cast("long") <= 0))
        .withColumn("is_negative_total", (coalesce_fn(col("total_amount"), lit(0.0)) < 0.0))
        .withColumn("is_passenger_missing", (coalesce_fn(col("passenger_count"), lit(0)) <= 0))
    )

    # 6) Stable row hash (supports idempotent downstream merges)
    base_cols = [
        "vendor_id", "pickup_ts", "dropoff_ts", "passenger_count", "trip_distance",
        "ratecode_id", "store_and_fwd_flag", "pu_zone_id", "do_zone_id",
        "payment_type", "fare_amount", "extra", "mta_tax", "tip_amount",
        "tolls_amount", "improvement_surcharge", "total_amount",
        "congestion_surcharge", "year", "month"
    ]
    computed_hash = sha2(concat_ws("|", *[col(c).cast("string") for c in base_cols]), 256)
    out = out.withColumn("row_hash", coalesce_fn(col("row_hash").cast("string"), computed_hash))

    # 7) Final projection to canonical schema
    out = out.select([col(c) for c in CANON_COLS]).persist(StorageLevel.MEMORY_AND_DISK)
    rows_out = out.count()

    # ====== WRITE ======
    cleanup_dir(out_dir)  # idempotent overwrite of the month folder

    num_out = max(1, int(a.target_files_per_month))
    writer_df = (out.repartition(num_out) if num_out > 1 else out.coalesce(1))
    partitions_out_planned = writer_df.rdd.getNumPartitions()

    writer_df.write.mode("overwrite").parquet(out_dir)
    wall = time.time() - t0

    # ====== SIZES ======
    bytes_in_mb  = _dir_size_mb(in_dir)
    bytes_out_mb = _dir_size_mb(out_dir)
    files_out = len([f for f in os.listdir(out_dir) if f.startswith("part-") and f.endswith(".parquet")])

    # ====== STOP & EVENTLOG ======
    spark.stop()
    ev = _eventlog_stats(eventlog_dir, app_id)

    spark_conf_map = {
        "parquet_codec": a.parquet_codec,
        "shuffle_partitions": a.shuffle_partitions,
        "max_partition_bytes": a.max_partition_bytes,
        "aqe": bool(a.aqe),
        "broadcast_small": False,
        "target_files_per_month": num_out,
    }

    # ====== METRICS (same JSON shape) ======
    metrics_dir = os.path.join(a.data_root, "metrics", "s3", f"exp={a.experiment_id}")
    if a.trial is not None:
        metrics_dir = os.path.join(metrics_dir, f"trial={a.trial}")
    metrics_dir = os.path.join(metrics_dir, f"year={a.year}", f"month={a.month}")
    os.makedirs(metrics_dir, exist_ok=True)

    met = {
        "step": "silver_prep",
        "year": a.year, "month": a.month,
        "rows_in": rows_in, "rows_out": rows_out,
        "bytes_in_mb": bytes_in_mb, "bytes_out_mb": bytes_out_mb,
        "wall_sec": wall,
        "partitions_in": partitions_in, "partitions_out_planned": partitions_out_planned,
        "files_out": files_out,
        "spark_conf": spark_conf_map,
        "app_id": app_id, "eventlog_dir": eventlog_dir,
        **ev
    }
    with open(os.path.join(metrics_dir, "silver_prep.json"), "w", encoding="utf-8") as f:
        json.dump(met, f, ensure_ascii=False, indent=2)

if __name__ == "__main__":
    main()
