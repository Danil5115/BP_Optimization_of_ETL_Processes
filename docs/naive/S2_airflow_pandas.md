# docs/naive/S2_airflow_pandas.md

> **S2 – Airflow + pandas (automatizace bez distribuovaných výpočtů)**  
> Cíl: předvést shodnou ETL logiku jako u S3, avšak s `PythonOperator` a knihovnou pandas namísto Spark/Docker. Získává se orchestrace (plánování, retry, auditovatelnost), výpočetní výkon zůstává na úrovni S1.

## 0. Předpoklady
- Airflow (Local/Docker) se sdíleným přístupem k `DATA_ROOT` (volume/mount).  
- Referenční slovníky v `DATA_ROOT/ref/...`.

## 1. Airflow Variables / Connections
- `DATA_ROOT` – kořenová cesta k datům.  
- (volitelně) `NYC_APP_TOKEN` pro SODA.  
- (volitelně) `dwh_postgres` – pokud se má `COPY` provést přes `PostgresHook`.

## 2. DAG (pseudokód; lze uložit jako `dags/naive_pandas.py`)
```python
from __future__ import annotations
import os, time, hashlib
import pandas as pd
from airflow import DAG
from airflow.operators.python import PythonOperator
import pendulum

DATA_ROOT = os.getenv('DATA_ROOT', '/opt/etl/data')
DS = 'm6nq-qud6'
YEAR = 2021
MONTH = 1

TARGET = [
  "vendor_id","pickup_ts","dropoff_ts","pickup_date_id","dropoff_date_id",
  "passenger_count","trip_distance","ratecode_id","store_and_fwd_flag",
  "pu_zone_id","do_zone_id","payment_type","fare_amount","extra","mta_tax",
  "tip_amount","tolls_amount","improvement_surcharge","total_amount",
  "congestion_surcharge","year","month","load_src","row_hash",
]

args = {"owner": "de", "retries": 0}
with DAG(
    dag_id="naive_pandas",
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    schedule=None,
    catchup=False,
    default_args=args,
    tags=["naive", "pandas"],
) as dag:

    def bronze(**_):
        src = f"{DATA_ROOT}/landing/{DS}/{YEAR}/{MONTH:02d}/{DS}_tripdata_{YEAR}-{MONTH:02d}.csv"
        outdir = f"{DATA_ROOT}/bronze/s2/year={YEAR}/month={MONTH}"
        os.makedirs(outdir, exist_ok=True)
        t0 = time.time()
        df = pd.read_csv(src)
        df.to_parquet(f"{outdir}/part-000.parquet", index=False)
        print(f"[S2 bronze] rows={len(df)} wall_sec={time.time()-t0:.2f}")

    def silver(**_):
        src = f"{DATA_ROOT}/bronze/s2/year={YEAR}/month={MONTH}/part-000.parquet"
        outdir = f"{DATA_ROOT}/silver/s2/year={YEAR}/month={MONTH}"
        os.makedirs(outdir, exist_ok=True)
        t0 = time.time()
        df = pd.read_parquet(src)
        rename = {
            'tpep_pickup_datetime': 'pickup_ts',
            'tpep_dropoff_datetime': 'dropoff_ts',
            'PULocationID': 'pu_zone_id',
            'DOLocationID': 'do_zone_id',
            'RatecodeID': 'ratecode_id',
            'VendorID': 'vendor_id',
        }
        df = df.rename(columns={k: v for k, v in rename.items() if k in df.columns})
        for c in ['vendor_id','ratecode_id','pu_zone_id','do_zone_id','payment_type','passenger_count']:
            if c in df.columns:
                df[c] = pd.to_numeric(df[c], errors='coerce').astype('Int64')
        for c in ['trip_distance','fare_amount','extra','mta_tax','tip_amount','tolls_amount','improvement_surcharge','total_amount','congestion_surcharge']:
            if c in df.columns:
                df[c] = pd.to_numeric(df[c], errors='coerce')
        for c in ['pickup_ts','dropoff_ts']:
            if c in df.columns:
                df[c] = pd.to_datetime(df[c], errors='coerce')
        if 'pickup_ts' in df.columns:
            df['year'] = df['pickup_ts'].dt.year
            df['month'] = df['pickup_ts'].dt.month
            df['pickup_date_id'] = df['pickup_ts'].dt.strftime('%Y%m%d').astype('int64')
        if 'dropoff_ts' in df.columns:
            df['dropoff_date_id'] = df['dropoff_ts'].dt.strftime('%Y%m%d').astype('int64')
        df.to_parquet(f"{outdir}/part-000.parquet", index=False)
        print(f"[S2 silver] rows={len(df)} wall_sec={time.time()-t0:.2f}")

    def join(**_):
        src = f"{DATA_ROOT}/silver/s2/year={YEAR}/month={MONTH}/part-000.parquet"
        outdir = f"{DATA_ROOT}/silver_wide/s2/year={YEAR}/month={MONTH}"
        os.makedirs(outdir, exist_ok=True)
        df = pd.read_parquet(src)
        zones = pd.read_parquet(f"{DATA_ROOT}/ref/zones/zones.parquet")
        rates = pd.read_parquet(f"{DATA_ROOT}/ref/ratecode/ratecode.parquet")
        pay = pd.read_parquet(f"{DATA_ROOT}/ref/payment_type/payment_type.parquet")
        ven = pd.read_parquet(f"{DATA_ROOT}/ref/vendor/vendor.parquet")
        if 'vendor_id' in df.columns:
            df = df.merge(ven, how='left', on='vendor_id')
        if 'ratecode_id' in df.columns:
            df = df.merge(rates, how='left', on='ratecode_id')
        if 'payment_type' in df.columns:
            df = df.merge(pay.rename(columns={'payment_type_id': 'payment_type', 'description': 'payment_desc'}), how='left', on='payment_type')
        if 'pu_zone_id' in df.columns:
            df = df.merge(zones.rename(columns={'zone_id': 'pu_zone_id', 'zone_name': 'pu_zone_name'}), how='left', on='pu_zone_id')
        if 'do_zone_id' in df.columns:
            df = df.merge(zones.rename(columns={'zone_id': 'do_zone_id', 'zone_name': 'do_zone_name'}), how='left', on='do_zone_id')
        df.to_parquet(f"{outdir}/part-000.parquet", index=False)
        print(f"[S2 join] rows={len(df)}")

    def export_csv(**_):
        src = f"{DATA_ROOT}/silver_wide/s2/year={YEAR}/month={MONTH}/part-000.parquet"
        outdir = f"{DATA_ROOT}/export/s2/year={YEAR}/month={MONTH}"
        os.makedirs(outdir, exist_ok=True)
        df = pd.read_parquet(src)
        df['load_src'] = 'S2'
        hcols = [
            "vendor_id","pickup_ts","dropoff_ts","passenger_count","trip_distance","ratecode_id",
            "store_and_fwd_flag","pu_zone_id","do_zone_id","payment_type","fare_amount","extra",
            "mta_tax","tip_amount","tolls_amount","improvement_surcharge","total_amount",
            "congestion_surcharge","year","month"
        ]
        for c in ['pickup_ts','dropoff_ts']:
            if c in df.columns:
                df[c] = pd.to_datetime(df[c], errors='coerce').dt.strftime('%Y-%m-%dT%H:%M:%S.%f').str[:-3]
        for c in hcols:
            if c not in df.columns:
                df[c] = None
        df['row_hash'] = df[hcols].astype(str).agg('|'.join, axis=1).apply(lambda s: hashlib.sha256(s.encode('utf-8')).hexdigest())
        for c in TARGET:
            if c not in df.columns:
                df[c] = None
        df = df[TARGET]
        csv = f"{outdir}/fact_trips_{YEAR}_{MONTH:02d}.csv"
        df.to_csv(csv, index=False)
        print(f"[S2 export] -> {csv}")

    def dwh_copy(**_):
        path = f"{DATA_ROOT}/export/s2/year={YEAR}/month={MONTH}/fact_trips_{YEAR}_{MONTH:02d}.csv"
        print("COPY do DWH (ilustrativně):")
        print("COPY dds.fact_trips (...) FROM STDIN WITH (FORMAT CSV, HEADER TRUE);")
        print(f"# např. psql -c \"\copy dds.fact_trips (...) FROM '{path}' CSV HEADER\"")

    t_bronze = PythonOperator(task_id='bronze', python_callable=bronze)
    t_silver = PythonOperator(task_id='silver', python_callable=silver)
    t_join   = PythonOperator(task_id='join',   python_callable=join)
    t_export = PythonOperator(task_id='export', python_callable=export_csv)
    t_copy   = PythonOperator(task_id='dwh_copy', python_callable=dwh_copy)

    t_bronze >> t_silver >> t_join >> t_export >> t_copy
```
