from __future__ import annotations

import argparse
from typing import Dict

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F, types as T

# -- Strict schemas for small reference dictionaries (CSV -> typed DataFrame)
SCHEMAS: Dict[str, T.StructType] = {
    "vendor": T.StructType([
        T.StructField("vendor_id",   T.IntegerType(), False),
        T.StructField("vendor_name", T.StringType(),  True),
    ]),
    "ratecode": T.StructType([
        T.StructField("ratecode_id", T.IntegerType(), False),
        T.StructField("description", T.StringType(),  True),
    ]),
    "payment_type": T.StructType([
        T.StructField("payment_type_id", T.IntegerType(), False),
        T.StructField("description",     T.StringType(),  True),
    ]),
    "store_flag": T.StructType([
        T.StructField("store_flag",  T.StringType(), True),
        T.StructField("description", T.StringType(), True),
    ]),
    # Matches the CSV we generate in init_dims (names must stay consistent)
    "dim_date": T.StructType([
        T.StructField("date_id",         T.IntegerType(), False),
        T.StructField("date",            T.StringType(),  False),  # ISO yyyy-MM-dd (keep as text to avoid TZ issues)
        T.StructField("year",            T.IntegerType(), False),
        T.StructField("quarter",         T.IntegerType(), False),
        T.StructField("month",           T.IntegerType(), False),
        T.StructField("day",             T.IntegerType(), False),
        T.StructField("iso_week",        T.IntegerType(), False),
        T.StructField("iso_dow",         T.IntegerType(), False),
        T.StructField("is_weekend",      T.BooleanType(), False),
        T.StructField("is_month_start",  T.BooleanType(), False),
        T.StructField("is_month_end",    T.BooleanType(), False),
    ]),
}

def read_and_normalize(spark: SparkSession, dataset: str, input_path: str) -> DataFrame:
    """
    Read CSV and return a normalized DataFrame for the given dataset.
    - zones: TLC raw -> rename + cast
    - others: load with strict schema for stable downstream types
    """
    if dataset == "zones":
        # Expect TLC columns: LocationID,Borough,Zone,service_zone
        df = (
            spark.read
            .option("header", True)
            .csv(input_path)
            .withColumnRenamed("LocationID",   "zone_id")
            .withColumnRenamed("Borough",      "borough")
            .withColumnRenamed("Zone",         "zone_name")
            .withColumnRenamed("service_zone", "service_zone")
            .withColumn("zone_id", F.col("zone_id").cast(T.IntegerType()))
        )
        # Keep column order explicit
        return df.select("zone_id", "borough", "zone_name", "service_zone")

    # For small dictionaries: enforce schema to avoid drift and implicit casts
    schema = SCHEMAS.get(dataset)
    if not schema:
        raise ValueError(f"Unknown dataset: {dataset}")

    return (
        spark.read
        .option("header", True)
        .schema(schema)
        .csv(input_path)
    )

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--dataset", required=True,
                    choices=["vendor", "ratecode", "payment_type", "store_flag", "zones", "dim_date"])
    ap.add_argument("--input",  required=True, help="CSV input path")
    ap.add_argument("--output", required=True, help="Parquet folder output")
    args = ap.parse_args()

    # Small, local Spark for one-off conversions
    spark = (
        SparkSession.builder
        .appName(f"ref_to_parquet_{args.dataset}")
        .config("spark.master", "local[*]")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    df = read_and_normalize(spark, args.dataset, args.input)

    # Single file per dictionary keeps layout predictable and easy to ship
    (df.coalesce(1)
       .write
       .mode("overwrite")
       .parquet(args.output))

    spark.stop()

if __name__ == "__main__":
    main()
