# docs/naive/S1_script_pipeline.md

> **S1 – Naivní skriptová sestava (bez Airflow, bez Spark)**  
> Cíl: předvést minimální ruční pipeline se stejnou logikou kroků (download → bronze → silver → join → export → DWH COPY) bez orchestrace a bez distribuovaných výpočtů.

## 0. Předpoklady
- Bash (Linux/macOS/WSL).
- Python 3.10+ s balíčky: `pandas`, `pyarrow`, `psycopg2-binary`, `requests`.
- Postgres DWH dostupný přes `psql`.
- Referenční slovníky (vendor, ratecode, payment_type, store_flag, zones, dim_date) v `DATA_ROOT/ref/...`.

## 1. Proměnné prostředí a výstupní struktura
```bash
export DATA_ROOT="/opt/etl/data"
export SODA_BASE="https://data.cityofnewyork.us/resource"
export NYC_APP_TOKEN="REPLACE_ME"
export DS="m6nq-qud6"
export YEAR=2021
export MONTH=01

export PGHOST="localhost"
export PGPORT=5432
export PGDATABASE="dwh"
export PGUSER="de"
export PGPASSWORD="de_password"
```

Vytvářené výstupy:
```
$DATA_ROOT/
  landing/<dataset>/<YYYY>/<MM>/...
  bronze/s1/year=<YYYY>/month=<M>/part-000.parquet
  silver/s1/year=<YYYY>/month=<M>/part-000.parquet
  silver_wide/s1/year=<YYYY>/month=<M>/part-000.parquet
  export/s1/year=<YYYY>/month=<M>/fact_trips_YYYY_MM.csv
```

## 2. Stažení dat (SODA)
```bash
mkdir -p "$DATA_ROOT/landing/$DS/$YEAR/$MONTH"
OUT="$DATA_ROOT/landing/$DS/$YEAR/$MONTH/${DS}_tripdata_${YEAR}-${MONTH}.csv"

curl -sSL -H "X-App-Token: $NYC_APP_TOKEN" \
  "$SODA_BASE/$DS.csv?$select=*&$where=tpep_pickup_datetime between '%23%23${YEAR}-${MONTH}-01T00:00:00' and '%23%23${YEAR}-${MONTH}-31T23:59:59'&$order=tpep_pickup_datetime" \
  -o "$OUT"
```

## 3. Bronze: CSV → Parquet
```bash
python - <<'PY'
import os, pandas as pd, pyarrow as pa, pyarrow.parquet as pq
DATA_ROOT=os.environ['DATA_ROOT']; DS=os.environ['DS']; Y=os.environ['YEAR']; M=os.environ['MONTH']
src=f"{DATA_ROOT}/landing/{DS}/{Y}/{M}/{DS}_tripdata_{Y}-{M}.csv"
outdir=f"{DATA_ROOT}/bronze/s1/year={Y}/month={int(M):d}"
os.makedirs(outdir, exist_ok=True)

df=pd.read_csv(src)
pa_tbl=pa.Table.from_pandas(df, preserve_index=False)
pq.write_table(pa_tbl, f"{outdir}/part-000.parquet", compression="snappy")
PY
```

## 4. Silver: základní čištění a typování
```bash
python - <<'PY'
import os, pandas as pd
DATA_ROOT=os.environ['DATA_ROOT']; Y=os.environ['YEAR']; M=int(os.environ['MONTH'])
bronze=f"{DATA_ROOT}/bronze/s1/year={Y}/month={M}/part-000.parquet"
outdir=f"{DATA_ROOT}/silver/s1/year={Y}/month={M}"
os.makedirs(outdir, exist_ok=True)

df=pd.read_parquet(bronze)
rename={'tpep_pickup_datetime':'pickup_ts','tpep_dropoff_datetime':'dropoff_ts','PULocationID':'pu_zone_id','DOLocationID':'do_zone_id','RatecodeID':'ratecode_id','VendorID':'vendor_id'}
df=df.rename(columns={k:v for k,v in rename.items() if k in df.columns})

for c in ['vendor_id','ratecode_id','pu_zone_id','do_zone_id','payment_type','passenger_count']:
    if c in df.columns: df[c]=pd.to_numeric(df[c], errors='coerce').astype('Int64')
for c in ['trip_distance','fare_amount','extra','mta_tax','tip_amount','tolls_amount','improvement_surcharge','total_amount','congestion_surcharge']:
    if c in df.columns: df[c]=pd.to_numeric(df[c], errors='coerce')
for c in ['pickup_ts','dropoff_ts']:
    if c in df.columns: df[c]=pd.to_datetime(df[c], errors='coerce')

if 'pickup_ts' in df.columns:
    df['year']=df['pickup_ts'].dt.year
    df['month']=df['pickup_ts'].dt.month
    df['pickup_date_id']=df['pickup_ts'].dt.strftime('%Y%m%d').astype('int64')
if 'dropoff_ts' in df.columns:
    df['dropoff_date_id']=df['dropoff_ts'].dt.strftime('%Y%m%d').astype('int64')

df.to_parquet(f"{outdir}/part-000.parquet", index=False)
PY
```

## 5. Join referencí
```bash
python - <<'PY'
import os, pandas as pd
DATA_ROOT=os.environ['DATA_ROOT']; Y=os.environ['YEAR']; M=int(os.environ['MONTH'])
silver=f"{DATA_ROOT}/silver/s1/year={Y}/month={M}/part-000.parquet"
wide_dir=f"{DATA_ROOT}/silver_wide/s1/year={Y}/month={M}"
os.makedirs(wide_dir, exist_ok=True)

df=pd.read_parquet(silver)
zones=pd.read_parquet(f"{DATA_ROOT}/ref/zones/zones.parquet")
rates=pd.read_parquet(f"{DATA_ROOT}/ref/ratecode/ratecode.parquet")
pay=pd.read_parquet(f"{DATA_ROOT}/ref/payment_type/payment_type.parquet")
ven=pd.read_parquet(f"{DATA_ROOT}/ref/vendor/vendor.parquet")

if 'vendor_id' in df.columns and 'vendor_id' in ven.columns:
    df=df.merge(ven, how='left', on='vendor_id')
if 'ratecode_id' in df.columns and 'ratecode_id' in rates.columns:
    df=df.merge(rates, how='left', on='ratecode_id')
if 'payment_type' in df.columns and 'payment_type_id' in pay.columns:
    df=df.merge(pay.rename(columns={'payment_type_id':'payment_type','description':'payment_desc'}), how='left', on='payment_type')
if 'pu_zone_id' in df.columns:
    df=df.merge(zones.rename(columns={'zone_id':'pu_zone_id','zone_name':'pu_zone_name'}), how='left', on='pu_zone_id')
if 'do_zone_id' in df.columns:
    df=df.merge(zones.rename(columns={'zone_id':'do_zone_id','zone_name':'do_zone_name'}), how='left', on='do_zone_id')

out=f"{wide_dir}/part-000.parquet"
df.to_parquet(out, index=False)
PY
```

## 6. Export: jednotné CSV pro DWH COPY
```bash
python - <<'PY'
import os, pandas as pd, hashlib
DATA_ROOT=os.environ['DATA_ROOT']; Y=int(os.environ['YEAR']); M=int(os.environ['MONTH'])
wide=f"{DATA_ROOT}/silver_wide/s1/year={Y}/month={M}/part-000.parquet"
outdir=f"{DATA_ROOT}/export/s1/year={Y}/month={M}"
os.makedirs(outdir, exist_ok=True)

TARGET=[
 "vendor_id","pickup_ts","dropoff_ts","pickup_date_id","dropoff_date_id",
 "passenger_count","trip_distance","ratecode_id","store_and_fwd_flag",
 "pu_zone_id","do_zone_id","payment_type","fare_amount","extra","mta_tax",
 "tip_amount","tolls_amount","improvement_surcharge","total_amount",
 "congestion_surcharge","year","month","load_src","row_hash"
]

df=pd.read_parquet(wide)
df['load_src']='S1'

hcols=["vendor_id","pickup_ts","dropoff_ts","passenger_count","trip_distance","ratecode_id","store_and_fwd_flag","pu_zone_id","do_zone_id","payment_type","fare_amount","extra","mta_tax","tip_amount","tolls_amount","improvement_surcharge","total_amount","congestion_surcharge","year","month"]
df['row_hash']=df[hcols].astype(str).agg('|'.join, axis=1).apply(lambda s: hashlib.sha256(s.encode('utf-8')).hexdigest())

for c in ['pickup_ts','dropoff_ts']:
    if c in df.columns:
        df[c]=pd.to_datetime(df[c], errors='coerce').dt.strftime('%Y-%m-%dT%H:%M:%S.%f').str[:-3]

for c in TARGET:
    if c not in df.columns: df[c]=None

df=df[TARGET]

csv=f"{outdir}/fact_trips_{Y}_{M:02d}.csv"
df.to_csv(csv, index=False)
PY
```

## 7. Načtení do DWH (COPY)
```bash
psql <<SQL
\timing on
COPY dds.fact_trips (
  vendor_id, pickup_ts, dropoff_ts, pickup_date_id, dropoff_date_id,
  passenger_count, trip_distance, ratecode_id, store_and_fwd_flag, pu_zone_id, do_zone_id,
  payment_type, fare_amount, extra, mta_tax, tip_amount, tolls_amount, improvement_surcharge,
  total_amount, congestion_surcharge, year, month, load_src, row_hash
) FROM PROGRAM 'cat "$DATA_ROOT/export/s1/year='${YEAR}'/month='${MONTH#0}'/fact_trips_'${YEAR}'_'${MONTH}'.csv"' WITH (FORMAT CSV, HEADER TRUE, DELIMITER ',');
SQL
```
---