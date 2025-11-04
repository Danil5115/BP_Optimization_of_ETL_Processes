from __future__ import annotations

import argparse
import json
import os
import time
import glob
import io
from typing import List, Optional, Tuple, Dict, Any

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    col, broadcast as bcast, lit, upper, trim,
    to_date, to_timestamp, make_date, date_format, weekofyear, last_day, when,
    dayofweek,
)

# ---------- CLI ----------
def argp():
    p = argparse.ArgumentParser(description="Wide join (facts + dims)")
    p.add_argument("--year", type=int, required=True)
    p.add_argument("--month", type=int, required=True)
    p.add_argument("--data-root", required=True)
    p.add_argument("--experiment-id", required=True)
    p.add_argument("--trial", type=int)

    # Perf flags (match DAG)
    p.add_argument("--parquet-codec", default="snappy")
    p.add_argument("--shuffle-partitions", type=int, default=64)
    p.add_argument("--max-partition-bytes", type=int, default=128 * 1024 * 1024)
    p.add_argument("--aqe", action="store_true")
    p.add_argument("--broadcast-small", action="store_true",
                   help="force broadcast() for small dims")
    p.add_argument("--disable-auto-broadcast", action="store_true",
                   help="set spark.sql.autoBroadcastJoinThreshold=-1 (baseline w/o auto-broadcast)")

    # Optional explicit dim paths (override defaults)
    p.add_argument("--zones-path")
    p.add_argument("--payments-path")
    p.add_argument("--ratecodes-path")
    p.add_argument("--vendors-path")
    p.add_argument("--storeflag-path")
    p.add_argument("--dimdate-path")
    return p.parse_args()

# ---------- utils ----------
def sum_dir_bytes(path: str) -> int:
    """Sum file sizes under path (bytes). Safe on missing paths."""
    if not path or not os.path.exists(path):
        return 0
    total = 0
    for root, _, files in os.walk(path):
        for f in files:
            try:
                total += os.path.getsize(os.path.join(root, f))
            except FileNotFoundError:
                pass
    return total

def ref_path(data_root: str, name: str) -> str:
    """Resolve default parquet path: data/ref/<name>/<name>.parquet"""
    return os.path.join(data_root, "ref", name, f"{name}.parquet")

def first_existing(paths: List[str]) -> Optional[str]:
    """Pick the first existing path from candidates."""
    for p in paths:
        if p and os.path.exists(p):
            return p
    return None

def cleanup_dir(path: str) -> None:
    """Best-effort remove all files/dirs under given path (keep the root)."""
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

def silver_in_dir(a) -> str:
    """
    Find existing base silver folder for (year, month):
      1) .../silver/s3/exp=E/trial=T/year=Y/month=M  (if trial set)
      2) .../silver/s3/exp=E/year=Y/month=M          (fallback)
    """
    base = os.path.join(a.data_root, "silver", "s3", f"exp={a.experiment_id}")
    candidates = []
    if a.trial is not None:
        candidates.append(os.path.join(base, f"trial={int(a.trial)}", f"year={a.year}", f"month={a.month}"))
    candidates.append(os.path.join(base, f"year={a.year}", f"month={a.month}"))

    p = first_existing(candidates)
    if p:
        return p
    raise FileNotFoundError("No silver input found. Tried:\n  " + "\n  ".join(candidates))

def _eventlog_stats(eventlog_dir: str, app_id: str) -> Dict[str, Any]:
    """Pull shuffle/GC/task stats from Spark event log of current app."""
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
        mx = max(run_times); mean = sum(run_times) / len(run_times)
        if mean > 0:
            skew = round(mx / mean, 6)
    return {
        "shuffle_read_mb": round(read_b / mb, 3),
        "shuffle_write_mb": round(write_b / mb, 3),
        "tasks_total": tasks, "stages": len(stages_seen),
        "disk_spill_mb": round(disk_spill / mb, 3), "memory_spill_mb": round(mem_spill / mb, 3),
        "gc_time_ms": gc_ms, "executor_run_time_ms": run_ms, "skew_factor": skew
    }

# ---- Normalizers for each dim (tolerant to column names) ----
def normalize_zones(df: DataFrame) -> DataFrame:
    id_cands   = ["zone_id", "locationid", "LocationID", "Location_Id", "PULocationID", "DOLocationID"]
    name_cands = ["zone_name", "Zone", "zone", "service_zone", "ServiceZone"]
    bor_cands  = ["borough", "Borough"]
    def pick(cands, cols): return next((c for c in cands if c in cols), None)
    idc   = pick(id_cands, df.columns) or "zone_id"
    namec = pick(name_cands, df.columns)
    borc  = pick(bor_cands, df.columns)
    out = df
    if idc != "zone_id": out = out.withColumnRenamed(idc, "zone_id")
    if namec and namec != "zone_name": out = out.withColumnRenamed(namec, "zone_name")
    if borc and borc != "borough": out = out.withColumnRenamed(borc, "borough")
    out = (out
           .withColumn("zone_id", col("zone_id").cast("int"))
           .select([c for c in ["zone_id", "zone_name", "borough"] if c in out.columns])
           .dropDuplicates(["zone_id"]))
    if "zone_name" not in out.columns: out = out.withColumn("zone_name", lit(None).cast("string"))
    if "borough" not in out.columns: out = out.withColumn("borough", lit(None).cast("string"))
    return out

def normalize_payments(df: DataFrame) -> DataFrame:
    id_cands   = ["payment_type", "paymenttype", "payment_type_id", "paymenttypeid", "PaymentTypeID"]
    name_cands = ["payment_desc", "payment_description", "description", "PaymentDesc"]
    def pick(cands, cols): return next((c for c in cands if c in cols), None)
    idc   = pick(id_cands, df.columns) or "payment_type"
    namec = pick(name_cands, df.columns)
    out = df
    if idc != "payment_type": out = out.withColumnRenamed(idc, "payment_type")
    out = out.withColumn("payment_type", col("payment_type").cast("int"))
    if namec and namec != "payment_desc": out = out.withColumnRenamed(namec, "payment_desc")
    if "payment_desc" not in out.columns: out = out.withColumn("payment_desc", lit(None).cast("string"))
    return out.select("payment_type", "payment_desc").dropDuplicates(["payment_type"])

def normalize_ratecodes(df: DataFrame) -> DataFrame:
    id_cands   = ["ratecode_id", "ratecodeid", "RatecodeID"]
    name_cands = ["ratecode_desc", "ratecode_description", "description", "RatecodeDesc"]
    def pick(cands, cols): return next((c for c in cands if c in cols), None)
    idc   = pick(id_cands, df.columns) or "ratecode_id"
    namec = pick(name_cands, df.columns)
    out = df
    if idc != "ratecode_id": out = out.withColumnRenamed(idc, "ratecode_id")
    out = out.withColumn("ratecode_id", col("ratecode_id").cast("int"))
    if namec and namec != "ratecode_desc": out = out.withColumnRenamed(namec, "ratecode_desc")
    if "ratecode_desc" not in out.columns: out = out.withColumn("ratecode_desc", lit(None).cast("string"))
    return out.select("ratecode_id", "ratecode_desc").dropDuplicates(["ratecode_id"])

def normalize_vendor(df: DataFrame) -> DataFrame:
    id_cands   = ["vendor_id", "vendorid", "VendorID"]
    name_cands = ["vendor_name", "vendor", "description", "VendorName"]
    def pick(cands, cols): return next((c for c in cands if c in cols), None)
    idc   = pick(id_cands, df.columns) or "vendor_id"
    namec = pick(name_cands, df.columns)
    out = df
    if idc != "vendor_id": out = out.withColumnRenamed(idc, "vendor_id")
    out = out.withColumn("vendor_id", col("vendor_id").cast("int"))
    if namec and namec != "vendor_name": out = out.withColumnRenamed(namec, "vendor_name")
    if "vendor_name" not in out.columns: out = out.withColumn("vendor_name", lit(None).cast("string"))
    return out.select("vendor_id", "vendor_name").dropDuplicates(["vendor_id"])

def normalize_store_flag(df: DataFrame) -> DataFrame:
    flag_cands = ["store_flag", "store_and_fwd_flag", "flag"]
    desc_cands = ["description", "desc", "store_flag_desc"]
    def pick(cands, cols): return next((c for c in cands if c in cols), None)
    fc = pick(flag_cands, df.columns) or "store_flag"
    dc = pick(desc_cands, df.columns)
    out = df
    if fc != "store_flag": out = out.withColumnRenamed(fc, "store_flag")
    if dc and dc != "description": out = out.withColumnRenamed(dc, "description")
    out = (out
           .withColumn("store_flag", upper(trim(col("store_flag").cast("string"))))
           .select("store_flag", *(["description"] if "description" in out.columns else []))
           .dropDuplicates(["store_flag"]))
    if "description" not in out.columns: out = out.withColumn("description", lit(None).cast("string"))
    return out

def normalize_dim_date(df: DataFrame) -> DataFrame:
    """Make dim_date schema consistent (id, date, iso fields, edges)."""
    out = df
    # id aliases
    if "DateId" in out.columns: out = out.withColumnRenamed("DateId", "date_id")
    if "dateid" in out.columns: out = out.withColumnRenamed("dateid", "date_id")
    if "date_key" in out.columns and "date_id" not in out.columns: out = out.withColumnRenamed("date_key", "date_id")
    # date column
    if "date" in out.columns:
        out = out.withColumn("date", to_date(col("date")))
    elif "full_date" in out.columns:
        out = out.withColumn("date", to_date(to_timestamp(col("full_date"))))
    elif all(c in out.columns for c in ["year", "month", "day"]):
        out = out.withColumn("date", make_date(col("year").cast("int"), col("month").cast("int"), col("day").cast("int")))
    # date_id
    if "date_id" not in out.columns:
        if "date" in out.columns:
            out = out.withColumn("date_id", date_format(col("date"), "yyyyMMdd").cast("int"))
        elif all(c in out.columns for c in ["year", "month", "day"]):
            out = out.withColumn(
                "date_id",
                (col("year").cast("int") * 10000 + col("month").cast("int") * 100 + col("day").cast("int"))
            )
    # iso helpers and month edges
    if "iso_dow" not in out.columns:
        if "date" in out.columns:
            out = out.withColumn("iso_dow", (((dayofweek(col("date")) + lit(5)) % lit(7)) + lit(1)).cast("int"))
        elif "day_of_week" in out.columns:
            out = out.withColumnRenamed("day_of_week", "iso_dow").withColumn("iso_dow", col("iso_dow").cast("int"))
    if "iso_week" not in out.columns:
        if "date" in out.columns:
            out = out.withColumn("iso_week", weekofyear(col("date")).cast("int"))
        elif "week_of_year" in out.columns:
            out = out.withColumnRenamed("week_of_year", "iso_week").withColumn("iso_week", col("iso_week").cast("int"))
    if "is_weekend" not in out.columns:
        out = out.withColumn("is_weekend", (col("iso_dow") >= lit(6)) if "iso_dow" in out.columns else lit(None).cast("boolean"))
    if "is_month_start" not in out.columns:
        out = out.withColumn("is_month_start", (col("day").cast("int") == lit(1)) if "day" in out.columns else lit(None).cast("boolean"))
    if "is_month_end" not in out.columns:
        out = out.withColumn("is_month_end", (col("date") == last_day(col("date"))) if "date" in out.columns else lit(None).cast("boolean"))
    keep = [c for c in [
        "date_id", "date", "year", "quarter", "month", "day",
        "iso_week", "iso_dow", "is_weekend", "is_month_start", "is_month_end"
    ] if c in out.columns]
    return out.select(*keep).dropDuplicates(["date_id"])

# ---------- main ----------
def main():
    a = argp()

    # Input facts (silver)
    facts_dir = silver_in_dir(a)
    print(f"[silver_join] reading silver from: {facts_dir}")

    # Output wide folder
    out_base = os.path.join(a.data_root, "silver_wide", "s3", f"exp={a.experiment_id}")
    if a.trial is not None:
        out_base = os.path.join(out_base, f"trial={int(a.trial)}")
    out_dir = os.path.join(out_base, f"year={a.year}", f"month={a.month}")
    print(f"[silver_join] will write to: {out_dir}")

    # Event log for shuffle metrics
    eventlog_dir = os.path.join(a.data_root, "_spark_eventlog")
    os.makedirs(eventlog_dir, exist_ok=True)

    spark_builder = (
        SparkSession.builder
        .appName("silver_join")
        .config("spark.master", "local[*]")
        .config("spark.sql.shuffle.partitions", a.shuffle_partitions)
        .config("spark.sql.files.maxPartitionBytes", a.max_partition_bytes)
        .config("spark.sql.adaptive.enabled", "true" if a.aqe else "false")
        .config("spark.sql.parquet.compression.codec", a.parquet_codec)
        .config("spark.eventLog.enabled", "true")
        .config("spark.eventLog.dir", f"file://{eventlog_dir}")
        .config("spark.eventLog.compress", "false")
    )
    if a.disable_auto_broadcast:
        spark_builder = spark_builder.config("spark.sql.autoBroadcastJoinThreshold", -1)

    spark = spark_builder.getOrCreate()
    app_id = spark.sparkContext.applicationId
    spark.sparkContext.setLogLevel("WARN")

    t0 = time.time()

    # Facts
    f = spark.read.parquet(facts_dir)
    try:
        rows_in = f.count()
    except Exception:
        rows_in = None

    # Ensure stable types for join keys / flags
    f = (
        f.withColumn("pu_zone_id", col("pu_zone_id").cast("int"))
         .withColumn("do_zone_id", col("do_zone_id").cast("int"))
         .withColumn("payment_type", col("payment_type").cast("int"))
         .withColumn("ratecode_id",  col("ratecode_id").cast("int"))
         .withColumn("vendor_id",    col("vendor_id").cast("int"))
         .withColumn("store_and_fwd_flag", upper(trim(col("store_and_fwd_flag").cast("string"))))
    ).alias("f")

    # Resolve required dims (fail-fast if missing)
    zones_path     = a.zones_path     or ref_path(a.data_root, "zones")
    payments_path  = a.payments_path  or ref_path(a.data_root, "payment_type")
    ratecodes_path = a.ratecodes_path or ref_path(a.data_root, "ratecode")
    vendors_path   = a.vendors_path   or ref_path(a.data_root, "vendor")
    storeflag_path = a.storeflag_path or ref_path(a.data_root, "store_flag")
    dimdate_path   = a.dimdate_path   or ref_path(a.data_root, "dim_date")

    missing = [p for p in [zones_path, payments_path, ratecodes_path, vendors_path, storeflag_path, dimdate_path] if not os.path.exists(p)]
    if missing:
        raise FileNotFoundError(
            "Required dimension parquet(s) not found:\n  " + "\n  ".join(missing) +
            "\nExpected layout: data/ref/<name>/<name>.parquet."
        )

    # Read & normalize dims (be lenient to column names)
    zones     = normalize_zones(      spark.read.parquet(zones_path))
    pay       = normalize_payments(   spark.read.parquet(payments_path))
    rate      = normalize_ratecodes(  spark.read.parquet(ratecodes_path))
    vendor    = normalize_vendor(     spark.read.parquet(vendors_path))
    storeflag = normalize_store_flag( spark.read.parquet(storeflag_path))
    dim_date  = normalize_dim_date(   spark.read.parquet(dimdate_path))
    dd_pick   = dim_date.alias("dd_pick")
    dd_drop   = dim_date.alias("dd_drop")

    # Optional broadcast to cut shuffle on small dims
    Z  = bcast(zones)     if a.broadcast_small else zones
    P  = bcast(pay)       if a.broadcast_small else pay
    R  = bcast(rate)      if a.broadcast_small else rate
    V  = bcast(vendor)    if a.broadcast_small else vendor
    SF = bcast(storeflag) if a.broadcast_small else storeflag
    DP = bcast(dd_pick)   if a.broadcast_small else dd_pick
    DD = bcast(dd_drop)   if a.broadcast_small else dd_drop

    # Wide join (facts + all dims, LEFT to keep all facts)
    wide = (
        f
        .join(Z.alias("zpu"), col("f.pu_zone_id") == col("zpu.zone_id"), "left")
        .join(Z.alias("zdo"), col("f.do_zone_id") == col("zdo.zone_id"), "left")
        .join(P.alias("p"),   col("f.payment_type")     == col("p.payment_type"),  "left")
        .join(R.alias("r"),   col("f.ratecode_id")      == col("r.ratecode_id"),   "left")
        .join(V.alias("v"),   col("f.vendor_id")        == col("v.vendor_id"),     "left")
        .join(SF.alias("sf"), col("f.store_and_fwd_flag") == col("sf.store_flag"), "left")
        .join(DP, col("f.pickup_date_id")  == col("dd_pick.date_id"), "left")
        .join(DD, col("f.dropoff_date_id") == col("dd_drop.date_id"), "left")
    )

    # Select fact + decorated fields (explicit ordering)
    wide = wide.select(
        col("f.*"),
        col("zpu.zone_name").alias("pu_zone_name"),
        col("zpu.borough").alias("pu_borough"),
        col("zdo.zone_name").alias("do_zone_name"),
        col("zdo.borough").alias("do_borough"),
        col("p.payment_desc").alias("payment_desc"),
        col("r.ratecode_desc").alias("ratecode_desc"),
        col("v.vendor_name").alias("vendor_name"),
        col("sf.description").alias("store_flag_desc"),
        col("dd_pick.iso_dow").alias("pickup_iso_dow"),
        col("dd_pick.is_weekend").alias("pickup_is_weekend"),
        col("dd_drop.iso_dow").alias("dropoff_iso_dow"),
        col("dd_drop.is_weekend").alias("dropoff_is_weekend"),
    )

    # Clean only current wide folder (idempotent overwrite)
    cleanup_dir(out_dir)
    wide.write.mode("overwrite").parquet(out_dir)

    wall = time.time() - t0

    # Metrics
    try:
        rows_out = wide.count()
    except Exception:
        rows_out = None

    bytes_in = (
        sum_dir_bytes(facts_dir) + sum_dir_bytes(zones_path) + sum_dir_bytes(payments_path) +
        sum_dir_bytes(ratecodes_path) + sum_dir_bytes(vendors_path) + sum_dir_bytes(storeflag_path) +
        sum_dir_bytes(dimdate_path)
    )
    bytes_out = sum_dir_bytes(out_dir)

    spark.stop()
    ev = _eventlog_stats(eventlog_dir, app_id)

    # Persist metrics (same layout as before)
    met_dir = os.path.join(a.data_root, "metrics", "s3", f"exp={a.experiment_id}")
    if a.trial is not None:
        met_dir = os.path.join(met_dir, f"trial={int(a.trial)}")
    met_dir = os.path.join(met_dir, f"year={a.year}", f"month={a.month}")
    os.makedirs(met_dir, exist_ok=True)

    metrics = {
        "step": "silver_join",
        "year": a.year, "month": a.month,
        "rows_in": rows_in, "rows_out": rows_out,
        "bytes_in_mb": round(bytes_in / (1024 * 1024), 3),
        "bytes_out_mb": round(bytes_out / (1024 * 1024), 3),
        "wall_sec": wall,
        "spark_conf": {
            "parquet_codec": a.parquet_codec,
            "shuffle_partitions": a.shuffle_partitions,
            "max_partition_bytes": a.max_partition_bytes,
            "aqe": bool(a.aqe),
            "broadcast_small": bool(a.broadcast_small),
            "disable_auto_broadcast": bool(a.disable_auto_broadcast),
        },
        "app_id": app_id, "eventlog_dir": eventlog_dir,
        "facts_dir": facts_dir, "out_dir": out_dir,
        "zones_path": zones_path, "payments_path": payments_path, "ratecodes_path": ratecodes_path,
        "vendors_path": vendors_path, "storeflag_path": storeflag_path, "dimdate_path": dimdate_path,
        **ev
    }
    with open(os.path.join(met_dir, "silver_join.json"), "w", encoding="utf-8") as f_:
        json.dump(metrics, f_, ensure_ascii=False, indent=2)

if __name__ == "__main__":
    main()
