# Optimization of ETL Processes for Big Data Using Apache Airflow and Apache Spark

## Přehled
Projekt demonstruje **robustní a škálovatelný ETL řetězec** nad reálnými daty *NYC Yellow Taxi*, s důrazem na:
- orchestrace v **Apache Airflow** (DAGy pro landing/refs a hlavní benchmark),
- transformace v **Apache Spark** (Bronze → Silver → Silver-wide → Export),
- **Data Quality (DQ) gating** v průběhu pipeline i po nahrání do DWH,
- **měření výkonu a benchmarky** (čas, shuffle, I/O, WAL, propustnost),
- **DWH** v **PostgreSQL** s měsíčními partitiony a dvěma strategiemi loadu (single COPY vs. parallel + staging).

## Zadání
Tato bakalářská práce se zaměřuje na optimalizaci a automatizaci ETL (Extract, Transform, Load) procesů pro efektivní zpracování velkých dat s využitím Apache Airflow a Apache Spark. Cílem je navrhnout a implementovat robustní ETL proces nad datasetem NYC Yellow Taxi, porovnat výkonnost před/po optimalizacích a doložit přínosy. Součástí je správa paralelních úloh, možnosti optimalizace (AQE, broadcast, řízení počtu souborů), spolehlivé monitorování výkonu a DQ kontrol.

## Požadavky
- **Docker**
- 8+ GB RAM doporučeno (Spark + Airflow + Postgres + případně Metabase)
- Síťový přístup (pro stahování dat z NYC SODA API)

### Proměnné prostředí ( `.env` )
Doporučené proměnné, které můžeš upravit:
```
# cesty a obrazy
DATA_ROOT=/opt/etl/data
SPARK_IMAGE=my-spark:latest

# NYC SODA
NYC_APP_TOKEN=...      # zaregistrovat se pro použití NYC soda pro api
SODA_BASE=https://data.cityofnewyork.us/resource
SODA_PAGE_LIMIT=50000

# Postgres/DWH (příklad)
POSTGRES_HOST=postgres
POSTGRES_PORT=5432
POSTGRES_DB=dwh
POSTGRES_USER=de
POSTGRES_PASSWORD=de

# Je třeba nahradit cestu svou vlastní v počítači.
HOST_DATA_DIR=.../data
HOST_SPARK_JOBS_DIR=.../spark/jobs
```

> Pozn.: konkrétní porty/UI adresy se řídí `docker-compose.yml`.

## Instalace a spuštění
1. **Klonování repozitáře**
   ```bash
   git clone <URL repo>
   cd <repo>
   ```

2. **Nastavení `.env`**  
   Vytvoř (nebo uprav) `.env` dle výše uvedených klíčů. Výchozí hodnoty lze ponechat.

3. **Start přes Docker Compose**
   ```bash
   docker compose up -d --build
   ```
   Po startu ověř:
   - Airflow Web UI: `http://localhost:8080`  
   - Metabase: `http://localhost:3000` 
   - Postgres: `localhost:5432` 

4. **Airflow – základní nastavení**
   - **Variables** → nastav minimálně:
     - `DATA_ROOT` = `/opt/etl/data`
     - `SPARK_IMAGE` = `my-spark:latest`
     - (Landing DAG) `NYC_APP_TOKEN` (volitelné), `SODA_BASE`, `SODA_PAGE_LIMIT`
   - **Connections** → vytvoř `Conn Id: dwh_postgres`
     - Type: Postgres
     - Host: `postgres` (nebo `localhost`, dle Compose)
     - Port: `5432`
     - Schema: `dwh`
     - Login: `de`
     - Password: `de`

5. **Inicializace DWH a referenčních číselníků**
   - V Airflow spusť DAG **`init_dims`**  
     Ten vygeneruje `ref/*.csv`, převede je přes Spark do Parquet a nahraje do `dds.dim_*` přes `COPY`.

6. **Stažení landing dat (NYC SODA)**
   - Spusť DAG **`download_landing_soda`**  
     Na **Params** můžeš upravit `dataset`, `years`, `months` (výchozí v DAGu: 2021/1–3).  
     CSV se uloží do `data/landing/<dataset>/<YYYY>/<MM>/...`.

7. **Hlavní ETL/benchmark**
   - Spusť DAG **`s3_benchmark_act_act`**  
     Na **Params** je `default_conf` včetně optimalizačních voleb (AQE, `shuffle_partitions`, `max_partition_bytes`, `broadcast_small`, `target_files_per_month`), strategie exportu (`single_csv` vs. `parallel_parts`) a větve DWH loadu (`dwh_load`, `dwh_parallel.enabled`).  
     DQ pravidla a gating jsou v `default_conf.dq`.

## Účty a přístupy (příkladové)

- **Airflow**: `http://localhost:8080`  
  - Uživatelské jméno: `admin`  
  - Heslo: `admin`  

- **PostgreSQL (DWH)**: `localhost:5432`  
  - DB: `dwh`  
  - Uživatelské jméno: `de`  
  - Heslo: `de`  
  - Schémata: `dds`, `svc`  
  - Inicializační skripty: `dwh/init/00_create_dbs.sql`, `dwh/init/01_init_dwh.sql`  
  *(většinou se spustí automaticky přes Compose init, jinak spusť ručně přes `psql`)*

- **Metabase**: `http://localhost:3000`  
  - První přihlášení probíhá registrací admin uživatele ve webovém průvodci.  
  - Přidej Postgres jako zdroj dat (stejné parametry jako výše).

## Technické detaily
- **Airflow DAGy**
  - `init_dims` – generuje/normalizuje referenční CSV (`dim_date`, `zones`, atd.), převádí na Parquet (Spark `ref_to_parquet.py`) a nahrává do `dds.dim_*` přes `COPY`.
  - `download_landing_soda` – robustní stahování CSV z NYC SODA API s retry/backoff, ukládá do `data/landing/...`.
  - `s3_benchmark` – hlavní řetězec:
    - **Bronze**: `bronze_csv_to_parquet.py` (read CSV → Parquet, řízení počtu výstupních souborů, eventlog metriky).
    - **Silver**: `silver_trips_basic.py` (čištění/typizace), **Join**: `silver_join.py` (dimenze + volitelný **broadcast**).
    - **Export**: `export_fact_csv.py` (kanonický CSV pořádek sloupců, `single_csv` nebo `parallel_parts`, řiditelný počet partů).
    - **DWH load**: `dwh.py` – **single COPY** nebo **parallel** (staging tabulka + více paralelních COPY + INSERT do partišny, volitelný `ANALYZE`).
    - **DQ**: `dq_silver.py`, `dq_join.py`, `dq_dwh` – výpočty %, zápis do `svc.dq_results`, **gating** dle prahů v `default_conf.dq.gate_fail_on`.
    - **Měření/benchmark**: `svc.bench_results` a `metrics/metrics_airflow` obsahují metriky (wall time, bytes, shuffle R/W, spills, GC, tasky, WAL, velikost tabulek/Indexů, propustnost).
    - **Report**: automaticky se generuje `report.md` pro aktuální běh do `data/metrics_airflow/...`.

## Spuštění – rychlý postup
1. `init_dims` (jeden běh)  
2. `download_landing_soda` (zvol rok/měsíc)  
3. `s3_benchmark_act_act`  
   - **příklad parametrů** (`examples` v UI):
     - `baseline_soft` – bez AQE/broadcast, single CSV, DQ soft.
     - `opt_soft` – AQE + broadcast, single CSV.
     - `exp_parallel_soft` – AQE + broadcast, export do více partů + **parallel DWH load**.

## Struktura repozitáře
```
airflow/
  dags/
    s3_benchmark/
      __init__.py
      commands.py          # Jinja šablony pro spark-submit
      config.py            # DEFAULT_CONF + Docker operator config
      dag_s3_benchmark.py  # hlavní DAG (ETL, DQ, DWH load, report)
      dq.py                # DQ zápis + gating + DWH DQ
      dwh.py               # single/parallel load do Postgresu
      presets.py           # přednastavené scénáře (baseline/opt/exp)
      utils.py             # cesty, metriky → svc.bench_results
    dag_download_landing_soda.py.py  # landing loader přes NYC SODA
    dag_init_dims.py                 # generace/načtení referencí
  Dockerfile

data/
  _spark_eventlog/  # Spark eventlog (pro metriky)
  landing/          # stažené CSV z SODA
  bronze/ silver/ silver_wide/ export/
  metrics/ metrics_airflow/ ref/

docs/
  naive/
  figures/ Přílohy z BP

dwh/
  init/
    00_create_dbs.sql
    01_init_dwh.sql       # schémata, dimenze, fact atd
  metabase/      #SQL dotazy pro vytváření dashboardů a CSV souborů s výsledky běhů


spark/
  Dockerfile
  jars/
  jobs/
    bronze_csv_to_parquet.py
    silver_trips_basic.py
    silver_join.py
    export_fact_csv.py
    dq_silver.py
    dq_join.py
    ref_to_parquet.py

docker-compose.yml
.env
README.md
thesis.pdf
```

