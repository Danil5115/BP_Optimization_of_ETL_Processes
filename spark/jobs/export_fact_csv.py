from __future__ import annotations

import argparse
import os
import time
import json
import glob
import io
from typing import List, Tuple, Dict, Any

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, lit, to_timestamp, year as y_, month as m_,
    sha2, concat_ws, date_format, coalesce as coalesce_fn
)

# ===== Helpers: sizes & eventlog =====
def _dir_size_mb(path: str) -> float:
    """Walk directory tree and sum file sizes in MB."""
    total = 0
    for root, _, files in os.walk(path):
        for name in files:
            try:
                total += os.path.getsize(os.path.join(root, name))
            except OSError:
                pass
    return round(total / (1024 * 1024), 3)


def _eventlog_stats(eventlog_dir: str, app_id: str) -> Dict[str, Any]:
    """
    Read Spark event log for the app and extract shuffle / spill / gc / runtime stats.
    Falls back to {0} for all metrics if file not found.
    """
    path = os.path.join(eventlog_dir, app_id)
    if not os.path.exists(path):
        cands = glob.glob(os.path.join(eventlog_dir, f"{app_id}*"))
        path = cands[0] if cands else None
    if not path or not os.path.exists(path):
        return {k: 0 for k in [
            "shuffle_read_mb", "shuffle_write_mb", "tasks_total", "stages",
            "disk_spill_mb", "memory_spill_mb", "gc_time_ms", "executor_run_time_ms", "skew_factor"
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
                r = int(tm.get("Executor Run Time", 0))
                run_ms += r
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
        mx = max(run_times)
        mean = sum(run_times) / len(run_times)
        if mean > 0:
            skew = round(mx / mean, 6)

    return {
        "shuffle_read_mb": round(read_b / mb, 3),
        "shuffle_write_mb": round(write_b / mb, 3),
        "tasks_total": tasks,
        "stages": len(stages_seen),
        "disk_spill_mb": round(disk_spill / mb, 3),
        "memory_spill_mb": round(mem_spill / mb, 3),
        "gc_time_ms": gc_ms,
        "executor_run_time_ms": run_ms,
        "skew_factor": skew,
    }


# ===== Path resolution =====
def _resolve_silver_in_dir(a) -> str:
    """Prefer trial-aware path; fallback to no-trial. Error if neither exists."""
    base = os.path.join(a.data_root, "silver", "s3", f"exp={a.experiment_id}")
    with_trial = (os.path.join(base, f"trial={int(a.trial)}", f"year={a.year}", f"month={a.month}")
                  if a.trial is not None else None)
    no_trial = os.path.join(base, f"year={a.year}", f"month={a.month}")
    for p in (with_trial, no_trial):
        if p and os.path.exists(p):
            return p
    raise FileNotFoundError(f"No silver input found. Tried: {with_trial}, {no_trial}")


def _resolve_export_out_dir(a) -> str:
    """Export base path under export/s3/exp=.../(trial=...)/year=.../month=..."""
    base = os.path.join(a.data_root, "export", "s3", f"exp={a.experiment_id}")
    if a.trial is not None:
        base = os.path.join(base, f"trial={int(a.trial)}")
    return os.path.join(base, f"year={a.year}", f"month={a.month}")


# ===== Column mapping (target ordering for COPY to DWH) =====
TARGET_ORDER = [
    "vendor_id","pickup_ts","dropoff_ts","pickup_date_id","dropoff_date_id",
    "passenger_count","trip_distance","ratecode_id","store_and_fwd_flag",
    "pu_zone_id","do_zone_id","payment_type","fare_amount","extra","mta_tax",
    "tip_amount","tolls_amount","improvement_surcharge","total_amount",
    "congestion_surcharge","year","month","load_src","row_hash",
]

CANDIDATES = {
    "vendor_id": ["vendor_id","vendorid","VendorID"],
    "pickup_ts": ["pickup_ts","tpep_pickup_datetime","pickup_datetime","pickup_at"],
    "dropoff_ts": ["dropoff_ts","tpep_dropoff_datetime","dropoff_datetime","dropoff_at"],
    "pickup_date_id": ["pickup_date_id"],
    "dropoff_date_id": ["dropoff_date_id"],
    "passenger_count": ["passenger_count","passenger_cnt"],
    "trip_distance": ["trip_distance","trip_miles","trip_distance_mi"],
    "ratecode_id": ["ratecode_id","ratecodeid","RatecodeID"],
    "store_and_fwd_flag": ["store_and_fwd_flag","store_and_fwd"],
    "pu_zone_id": ["pu_zone_id","pulocationid","PULocationID","pickup_location_id"],
    "do_zone_id": ["do_zone_id","dolocationid","DOLocationID","dropoff_location_id"],
    "payment_type": ["payment_type","paymenttype"],
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
    "row_hash": ["row_hash","hash"],
}

INT_COLS = {"vendor_id","passenger_count","ratecode_id","pu_zone_id","do_zone_id",
            "payment_type","year","month","pickup_date_id","dropoff_date_id"}
DOUBLE_COLS = {"trip_distance","fare_amount","extra","mta_tax","tip_amount",
               "tolls_amount","improvement_surcharge","total_amount","congestion_surcharge"}
TS_COLS = {"pickup_ts","dropoff_ts"}
TS_FMT = "yyyy-MM-dd'T'HH:mm:ss.SSS"


# ===== Argparse =====
def parse_args():
    p = argparse.ArgumentParser()
    p.add_argument("--year", type=int, required=True)
    p.add_argument("--month", type=int, required=True)
    p.add_argument("--data-root", required=True)
    p.add_argument("--experiment-id", required=True)
    p.add_argument("--trial", type=int)
    p.add_argument("--target-files-per-month", type=int, default=1)
    p.add_argument("--measure-rows", action="store_true", default=False)
    # parallel_parts mode:
    p.add_argument("--save-parts", action="store_true", default=False,
                   help="if set, keep individual CSV parts under parts/ instead of merging")
    p.add_argument("--parts-subdir", default="parts",
                   help="subdirectory name under export/<...>/ to store part CSVs")
    return p.parse_args()


# ===== Small utilities for I/O =====
def first_existing(df, names: List[str]):
    """Pick the first existing source column among candidates."""
    for n in names:
        if n in df.columns:
            return col(n)
    return None


def _cleanup_dir(path: str) -> None:
    """Remove directory tree (best-effort). Used for tmp folders."""
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
    try:
        os.rmdir(path)
    except OSError:
        pass


def _collect_part_paths(tmp_out: str) -> List[str]:
    """Gather Spark CSV part files under tmp_out in a stable order."""
    part_paths: List[str] = []
    for root, _, files in os.walk(tmp_out):
        for f in files:
            if f.endswith(".csv"):
                part_paths.append(os.path.join(root, f))
    part_paths.sort()
    if not part_paths:
        raise RuntimeError("Export produced no CSV parts")
    return part_paths


def _merge_parts_to_single(part_paths: List[str], final_csv: str) -> float:
    """Merge CSV parts into a single file (skip headers for subsequent parts). Returns merge seconds."""
    t0 = time.time()
    with open(final_csv, "wb") as dst:
        for i, p in enumerate(part_paths):
            with open(p, "rb") as src:
                if i == 0:
                    dst.write(src.read())
                else:
                    first = True
                    for line in src:
                        if first:
                            first = False  # skip header
                            continue
                        dst.write(line)
    return time.time() - t0


def _copy_parts_to_dir(part_paths: List[str], parts_dir: str, year: int, month: int) -> float:
    """Copy parts into parts_dir with deterministic names. Returns total bytes in MB."""
    _cleanup_dir(parts_dir)
    os.makedirs(parts_dir, exist_ok=True)
    for i, p in enumerate(part_paths, start=1):
        dst = os.path.join(parts_dir, f"fact_trips_{year}_{month:02d}_part{i:03d}.csv")
        with open(p, "rb") as src, open(dst, "wb") as outf:
            outf.write(src.read())
    total_bytes = sum(os.path.getsize(os.path.join(parts_dir, f)) for f in os.listdir(parts_dir))
    return round(total_bytes / (1024 * 1024), 3)


# ===== Main =====
def main():
    a = parse_args()

    in_dir  = _resolve_silver_in_dir(a)
    out_dir = _resolve_export_out_dir(a)
    tmp_out = out_dir + ".__tmp"

    print(f"[export] reading from: {in_dir}")
    print(f"[export] will write to: {out_dir} (tmp: {tmp_out})")

    # Spark with eventLog enabled to capture shuffle metrics
    eventlog_dir = os.path.join(a.data_root, "_spark_eventlog")
    os.makedirs(eventlog_dir, exist_ok=True)

    spark = (
        SparkSession.builder
        .appName("export_fact_csv")
        .config("spark.master", "local[*]")
        .config("spark.eventLog.enabled", "true")
        .config("spark.eventLog.dir", f"file://{eventlog_dir}")
        .config("spark.eventLog.compress", "false")
        .getOrCreate()
    )
    app_id = spark.sparkContext.applicationId
    spark.sparkContext.setLogLevel("WARN")

    t0 = time.time()
    df = spark.read.parquet(in_dir)
    partitions_in = df.rdd.getNumPartitions()

    # --- Canonicalize columns to TARGET_ORDER (create missing columns as nulls)
    out = df
    for tgt in TARGET_ORDER:
        if tgt == "load_src":
            out = out.withColumn("load_src", lit(a.experiment_id))
            continue
        src = first_existing(out, CANDIDATES.get(tgt, []))
        out = out.withColumn(tgt, src if src is not None else lit(None))

    # --- Casts for target schema consistency
    for c in INT_COLS:
        if c in out.columns:
            out = out.withColumn(c, out[c].cast("int"))
    for c in DOUBLE_COLS:
        if c in out.columns:
            out = out.withColumn(c, out[c].cast("double"))

    # --- Timestamps -> ISO strings (Spark writes consistent text for COPY-friendly CSV)
    for c in TS_COLS:
        if c in out.columns:
            tmp = f"__{c}_tmp"
            out = out.withColumn(tmp, to_timestamp(col(c)))
            out = out.drop(c).withColumn(c, date_format(col(tmp), TS_FMT)).drop(tmp)

    # --- Backfill date ids and (year, month) from pickup_ts if missing
    ts_pick = to_timestamp(col("pickup_ts"), TS_FMT) if "pickup_ts" in out.columns else None
    ts_drop = to_timestamp(col("dropoff_ts"), TS_FMT) if "dropoff_ts" in out.columns else None
    if ts_pick is not None:
        out = out.withColumn("pickup_date_id", coalesce_fn(col("pickup_date_id"), date_format(ts_pick, "yyyyMMdd").cast("int")))
        out = out.withColumn("year",  coalesce_fn(col("year"),  y_(ts_pick)).cast("int"))
        out = out.withColumn("month", coalesce_fn(col("month"), m_(ts_pick)).cast("int"))
    if ts_drop is not None:
        out = out.withColumn("dropoff_date_id", coalesce_fn(col("dropoff_date_id"), date_format(ts_drop, "yyyyMMdd").cast("int")))

    # --- Row hash for change detection / idempotency downstream
    hash_cols = [
        "vendor_id","pickup_ts","dropoff_ts","passenger_count","trip_distance",
        "ratecode_id","store_and_fwd_flag","pu_zone_id","do_zone_id","payment_type",
        "fare_amount","extra","mta_tax","tip_amount","tolls_amount",
        "improvement_surcharge","total_amount","congestion_surcharge","year","month"
    ]
    out = out.drop("row_hash")
    out = out.withColumn("row_hash", sha2(concat_ws("|", *[col(c).cast("string") for c in hash_cols]), 256))

    out = out.select([col(c) for c in TARGET_ORDER])

    # --- Optional row counts (costly on big data; kept behind a flag)
    rows_in = rows_out = None
    if a.measure_rows:
        rows_in = out.count()
        rows_out = rows_in  # exporter does not filter rows

    # --- Ensure clean tmp output location
    _cleanup_dir(tmp_out)

    # --- Plan number of output files and write parts
    n_files = max(1, int(a.target_files_per_month))
    writer_df = (out.repartition(n_files) if n_files > 1 else out.coalesce(1))
    partitions_out_planned = writer_df.rdd.getNumPartitions()

    write_t0 = time.time()
    writer_df.write.mode("overwrite").option("header", True).csv(tmp_out)
    write_sec = time.time() - write_t0

    # --- Gather part files
    part_paths = _collect_part_paths(tmp_out)
    os.makedirs(out_dir, exist_ok=True)
    files_out = len(part_paths)

    final_csv = os.path.join(out_dir, f"fact_trips_{a.year}_{a.month:02d}.csv")
    merge_sec = 0.0

    # --- Two modes:
    # 1) parallel_parts -> copy parts to deterministic files under parts/
    # 2) single_csv / merge -> merge parts into a single CSV (skip headers on subsequent parts)
    if a.save_parts and n_files > 1:
        parts_dir = os.path.join(out_dir, a.parts_subdir)
        bytes_out_mb = _copy_parts_to_dir(part_paths, parts_dir, a.year, a.month)
    else:
        merge_sec = _merge_parts_to_single(part_paths, final_csv)
        bytes_out_mb = round(os.path.getsize(final_csv) / (1024 * 1024), 3)

    # --- Cleanup tmp
    _cleanup_dir(tmp_out)

    wall = time.time() - t0

    # --- Size metrics for input side
    bytes_in_mb = _dir_size_mb(in_dir)

    # --- Stop Spark and collect shuffle stats from eventlog
    spark.stop()
    ev = _eventlog_stats(eventlog_dir, app_id)

    # --- Persist metrics (schema kept unchanged)
    metrics_dir = os.path.join(a.data_root, "metrics", "s3", f"exp={a.experiment_id}")
    if a.trial is not None:
        metrics_dir = os.path.join(metrics_dir, f"trial={a.trial}")
    metrics_dir = os.path.join(metrics_dir, f"year={a.year}", f"month={a.month}")
    os.makedirs(metrics_dir, exist_ok=True)

    met = {
        "step": "export",
        "year": a.year, "month": a.month,
        "rows_in": rows_in, "rows_out": rows_out,
        "bytes_in_mb": bytes_in_mb, "bytes_out_mb": bytes_out_mb,
        "wall_sec": wall,
        "files_out": files_out,
        "partitions_in": partitions_in,
        "partitions_out_planned": partitions_out_planned,
        "merge_sec": merge_sec,
        "write_sec": write_sec,
        "spark_conf": {
            "target_files_per_month": int(a.target_files_per_month),
            "writer": ("single_csv" if (not a.save_parts and int(a.target_files_per_month) == 1)
                       else ("parallel_write_then_merge" if not a.save_parts else "parallel_parts")),
            "measure_rows": bool(a.measure_rows),
            **({"parts_subdir": a.parts_subdir} if a.save_parts else {}),
        },
        **ev
    }
    with open(os.path.join(metrics_dir, "export.json"), "w", encoding="utf-8") as f:
        json.dump(met, f, ensure_ascii=False, indent=2)


if __name__ == "__main__":
    main()
