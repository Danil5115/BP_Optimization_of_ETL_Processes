from __future__ import annotations

import os
import time
import json
import pathlib
import calendar
import random
import datetime as dt
from typing import Any, Dict, List, Tuple

import pendulum
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from airflow.decorators import dag, task
from airflow.models import Variable
from airflow.operators.python import get_current_context

# --- Config / Airflow Variables ---
DATA_ROOT   = Variable.get("DATA_ROOT", "/opt/etl/data")
LANDINGROOT = os.path.join(DATA_ROOT, "landing")
APP_TOKEN   = Variable.get("NYC_APP_TOKEN", "0FfsQRqTlu9gYGHpoi0wQbuRD")
SODA_BASE   = Variable.get("SODA_BASE", "https://data.cityofnewyork.us/resource")

DEFAULT_PLAN: Dict[str, Any] = {
    "dataset": "m6nq-qud6",                 # yellow taxi trips
    "date_field": "tpep_pickup_datetime",
    "years": [2021],
    "months": [1, 2, 3],
}

PAGE_LIMIT: int  = int(Variable.get("SODA_PAGE_LIMIT", "50000"))
MAX_RETRIES: int = 5
BACKOFF_SEC: int = 2
CONNECT_TIMEOUT: int = 10
READ_TIMEOUT: int = 600


# ------------- Helpers -------------
def _ensure_dir(p: str) -> None:
    """Create directory tree if it does not exist."""
    pathlib.Path(p).mkdir(parents=True, exist_ok=True)


def _month_bounds(year: int, month: int) -> Tuple[str, str]:
    """Return ISO boundaries (inclusive) for the given calendar month."""
    start = dt.datetime(year, month, 1, 0, 0, 0)
    last_day = calendar.monthrange(year, month)[1]
    end   = dt.datetime(year, month, last_day, 23, 59, 59)
    return start.strftime("%Y-%m-%dT%H:%M:%S"), end.strftime("%Y-%m-%dT%H:%M:%S")


def _make_session() -> requests.Session:
    """
    Build a requests.Session with retry policy for 429/5xx and
    respect of the Retry-After header.
    """
    sess = requests.Session()
    if APP_TOKEN:
        sess.headers.update({"X-App-Token": APP_TOKEN})
    retry = Retry(
        total=MAX_RETRIES,
        read=MAX_RETRIES,
        connect=MAX_RETRIES,
        status=MAX_RETRIES,
        backoff_factor=BACKOFF_SEC,                 # exponential backoff: 0.5, 1, 2, 4...
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=("GET",),                   # only idempotent GETs
        respect_retry_after_header=True,
        raise_on_status=False,
    )
    adapter = HTTPAdapter(max_retries=retry)
    sess.mount("http://", adapter)
    sess.mount("https://", adapter)
    return sess


def _exp_backoff_sleep(attempt: int) -> None:
    """Extra manual backoff (as a fallback): base * 2^(attempt-1) + small jitter."""
    wait = BACKOFF_SEC * (2 ** (attempt - 1)) + random.uniform(0, 0.3)
    time.sleep(wait)


def _soda_get_csv(
    session: requests.Session,
    dataset: str,
    date_field: str,
    start_iso: str,
    end_iso: str,
    offset: int,
    limit: int,
) -> requests.Response:
    """
    Return a Response streaming a CSV page. Uses the session with Retry; if
    we still get 429/5xx, perform a few manual attempts with extra backoff.
    """
    url = f"{SODA_BASE}/{dataset}.csv"
    params = {
        "$select": "*",
        "$where": f"{date_field} between '{start_iso}' and '{end_iso}'",
        "$order": f"{date_field}",
        "$limit": limit,
        "$offset": offset,
    }

    last_err: Exception | None = None
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            r = session.get(url, params=params, stream=True, timeout=(CONNECT_TIMEOUT, READ_TIMEOUT))
            # If urllib3 Retry didn't help and status is still bad — try again manually
            if r.status_code in (429, 500, 502, 503, 504):
                r.close()
                _exp_backoff_sleep(attempt)
                continue
            r.raise_for_status()
            return r
        except Exception as e:
            last_err = e
            _exp_backoff_sleep(attempt)

    raise RuntimeError(f"SODA failed after {MAX_RETRIES} retries: {url} params={params} error={last_err}")


def _merge_plan(dag_conf: Dict[str, Any] | None) -> Dict[str, Any]:
    """
    Merge user run config with defaults.

    Priority: DagRun.conf > Variable NYC_PLAN > DEFAULT_PLAN.

    Returns validated dict:
      {dataset, date_field, years[], months[], force}
    """
    conf = dag_conf or {}
    plan = json.loads(Variable.get("NYC_PLAN", json.dumps(DEFAULT_PLAN)))
    for k in ("dataset", "date_field", "years", "months"):
        if k in conf:
            plan[k] = conf[k]
    years = plan.get("years") or []
    months = plan.get("months") or []
    if not years or not months:
        raise ValueError("NYC plan must contain non-empty 'years' and 'months'.")
    return {
        "dataset": str(plan["dataset"]),
        "date_field": str(plan["date_field"]),
        "years": [int(y) for y in years],
        "months": [int(m) for m in months],
        "force": bool(conf.get("force", False)),
    }


# ------------- DAG -------------
@dag(
    dag_id="download_landing_soda",
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    schedule=None,
    catchup=False,
    tags=["landing", "download", "soda"],
)
def download_landing_soda():

    @task
    def build_jobs() -> list[dict]:
        """
        Build a JSON-serializable list of jobs:
          [{dataset, date_field, year, month, force}, ...]

        Priority: DagRun.conf > Variable NYC_PLAN > DEFAULT_PLAN
        """
        # 1) get conf from current DagRun
        ctx = get_current_context()
        dagrun = ctx.get("dag_run")
        conf: dict = (dagrun.conf or {}) if dagrun else {}

        # 2) base plan from Variable (or defaults)
        plan = json.loads(Variable.get("NYC_PLAN", json.dumps(DEFAULT_PLAN)))

        # 3) override keys coming from conf
        for k in ("dataset", "date_field", "years", "months"):
            if k in conf and conf[k] is not None:
                plan[k] = conf[k]
        force = bool(conf.get("force", False))

        # optional: debug log
        print(f"[build_jobs] final plan={plan}, force={force}")

        jobs = []
        for y in plan["years"]:
            for m in plan["months"]:
                jobs.append({
                    "dataset": plan["dataset"],
                    "date_field": plan["date_field"],
                    "year": int(y),
                    "month": int(m),
                    "force": force,
                })
        return jobs

    @task(retries=1, retry_delay=pendulum.duration(minutes=1))
    def download(job: Dict[str, Any]) -> str:
        """
        Download a single month to:
          landing/<dataset>/<YYYY>/<MM>/<dataset>_tripdata_YYYY-MM.csv

        Idempotency: if the target file already exists and force=False — skip.
        """
        dataset    = str(job["dataset"])
        date_field = str(job["date_field"])
        year       = int(job["year"])
        month      = int(job["month"])
        force      = bool(job.get("force", False))

        out_dir  = os.path.join(LANDINGROOT, dataset, f"{year}", f"{month:02d}")
        _ensure_dir(out_dir)
        out_name = f"{dataset}_tripdata_{year}-{month:02d}.csv"
        out_path = os.path.join(out_dir, out_name)

        if os.path.exists(out_path) and not force:
            print(f"[skip] exists: {out_path}")
            return out_path

        start_iso, end_iso = _month_bounds(year, month)
        print(f"[info] {dataset} {year}-{month:02d} where {date_field} between {start_iso} and {end_iso}")

        session = _make_session()
        tmp_path = out_path + ".part"
        offset = 0
        total_rows = 0

        try:
            with open(tmp_path, "wb") as f_out:
                while True:
                    resp = _soda_get_csv(session, dataset, date_field, start_iso, end_iso, offset, PAGE_LIMIT)

                    wrote_rows = 0
                    # First page: write header; subsequent pages: drop first line (header)
                    is_first_line = True
                    drop_first = (offset > 0)

                    for raw in resp.iter_lines(decode_unicode=True):
                        if not raw:
                            continue
                        if is_first_line:
                            is_first_line = False
                            if drop_first:
                                # skip header line on subsequent pages
                                continue
                        # normalize newline and write
                        f_out.write((raw + "\n").encode("utf-8"))
                        wrote_rows += 1

                    resp.close()

                    if wrote_rows == 0:  # no more data
                        break

                    offset += PAGE_LIMIT
                    total_rows += wrote_rows
                    print(f"[page] wrote={wrote_rows}, total={total_rows}, next offset={offset}")
                    time.sleep(0.2)

            # atomically rename temp file -> final target
            os.replace(tmp_path, out_path)
            size_mb = round(os.path.getsize(out_path) / (1024 * 1024), 3)
            print(f"[ok] saved: {out_path}  rows~{total_rows}  size_mb={size_mb}")
            return out_path

        finally:
            session.close()
            # cleanup partial file if something went wrong
            try:
                if os.path.exists(tmp_path) and not os.path.exists(out_path):
                    os.remove(tmp_path)
            except Exception:
                pass

    # ----- Graph -----
    jobs = build_jobs()            # list[dict] (XCom)
    _ = download.expand(job=jobs)  # dynamic task mapping


download_landing_soda()
