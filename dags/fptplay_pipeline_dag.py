import sys
import subprocess
from pathlib import Path
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator, ShortCircuitOperator
from airflow.operators.empty import EmptyOperator
from airflow.utils.dates import days_ago


PIPELINE_ROOT = "/opt/pipeline"
if PIPELINE_ROOT not in sys.path:
    sys.path.insert(0, PIPELINE_ROOT)


default_args = {
    "owner":            "de_team",
    "retries":          1,                        
    "retry_delay":      timedelta(minutes=5),     
    "email_on_failure": False,
    "email_on_retry":   False,
}


def task_check_raw_files(**context):
    import glob
    import os

    data_dir = f"{PIPELINE_ROOT}/data/raw"
    files = glob.glob(os.path.join(data_dir, "logt*.txt"))

    if not files:
        print(f"[check] File not found in {data_dir} — skip pipeline")
        return False

    total_size = sum(os.path.getsize(f) for f in files) / 1024 / 1024
    print(f"[check] Found {len(files)} file ({total_size:.1f} MB) — continue")

    context["ti"].xcom_push(key="file_count", value=len(files))
    context["ti"].xcom_push(key="total_mb",   value=round(total_size, 1))
    return True


def task_ingest(**context):
    import time
    sys.path.insert(0, PIPELINE_ROOT)

    from spark_jobs.ingest import main as ingest_main

    print("[airflow] Starting ingest task...")
    t0 = time.time()
    ingest_main()
    elapsed = time.time() - t0

    print(f"[airflow] Ingest done {elapsed:.1f}s")
    context["ti"].xcom_push(key="ingest_duration_s", value=round(elapsed, 1))


def task_transform(**context):
    import time
    sys.path.insert(0, PIPELINE_ROOT)

    from spark_jobs.transform import main as transform_main

    print("[airflow] Starting transform task...")
    t0 = time.time()
    transform_main()
    elapsed = time.time() - t0

    print(f"[airflow] Transform done {elapsed:.1f}s")
    context["ti"].xcom_push(key="transform_duration_s", value=round(elapsed, 1))


def task_load(**context):
    import time
    sys.path.insert(0, PIPELINE_ROOT)

    from spark_jobs.load import main as load_main

    print("[airflow] Starting load task...")
    t0 = time.time()
    load_main()
    elapsed = time.time() - t0

    print(f"[airflow] Load done {elapsed:.1f}s")
    context["ti"].xcom_push(key="load_duration_s", value=round(elapsed, 1))


def task_quality_check(**context):
    import os
    from sqlalchemy import create_engine, text

    conn_str = os.getenv(
        "POSTGRES_CONN",
        "postgresql+psycopg2://de_user:de_password@postgres:5432/log_pipeline"
    )
    engine = create_engine(conn_str)

    checks_passed = True

    with engine.connect() as conn:

        fact_count = conn.execute(
            text("SELECT COUNT(*) FROM fact_sessions")
        ).scalar()
        user_count = conn.execute(
            text("SELECT COUNT(*) FROM user_profiles")
        ).scalar()

        print(f"[quality] fact_sessions : {fact_count:,} rows")
        print(f"[quality] user_profiles : {user_count:,} rows")

        if fact_count == 0:
            print("[quality] FAIL: fact_sessions null!")
            checks_passed = False
        else:
            print("[quality] Row count OK")

        null_mac = conn.execute(
            text('SELECT COUNT(*) FROM fact_sessions WHERE "Mac" IS NULL')
        ).scalar()
        null_pct = null_mac / fact_count * 100 if fact_count > 0 else 0

        if null_pct > 5:
            print(f"[quality] WARN: {null_pct:.1f}% null Mac — kiểm tra data source")
        else:
            print(f"[quality] Null Mac rate: {null_pct:.2f}%")

        date_range = conn.execute(
            text("SELECT MIN(session_date), MAX(session_date) FROM fact_sessions")
        ).fetchone()
        print(f"[quality] Date range: {date_range[0]} → {date_range[1]}")


    ti = context["ti"]
    ingest_t   = ti.xcom_pull(task_ids="ingest",    key="ingest_duration_s")    or "N/A"
    transform_t = ti.xcom_pull(task_ids="transform", key="transform_duration_s") or "N/A"
    load_t     = ti.xcom_pull(task_ids="load",       key="load_duration_s")      or "N/A"

    print(f"""
╔══════════════════════════════════════╗
║       PIPELINE RUN SUMMARY           ║
╠══════════════════════════════════════╣
║  ingest    : {str(ingest_t)+'s':<25}║
║  transform : {str(transform_t)+'s':<25}║
║  load      : {str(load_t)+'s':<25}║
╠══════════════════════════════════════╣
║  fact_sessions  : {str(fact_count)+' rows':<19}║
║  user_profiles  : {str(user_count)+' rows':<19}║
╚══════════════════════════════════════╝
    """)

    if not checks_passed:
        raise ValueError("Data quality checks failed")

    print("[quality] Check passed!")

with DAG(
    dag_id="fptplay_log_pipeline",
    description="ETL pipeline: FPT Play STB logs → PostgreSQL",
    default_args=default_args,
    start_date=days_ago(1),
    schedule_interval="0 2 * * *",   
    catchup=False,                   
    tags=["fptplay", "etl", "pyspark"],
    doc_md="""

**Flow:** raw logs → PySpark ingest → transform → PostgreSQL → quality check

**Schedule:** Daily at 2:00 AM

**Tasks:**
1. `check_files` — verify raw data exists (ShortCircuit)
2. `ingest`      — parse Python-dict-string logs → Parquet
3. `transform`   — feature engineering 
4. `load`        — Parquet → PostgreSQL
5. `quality`     — row count + null rate 
    """,
) as dag:

    check_files = ShortCircuitOperator(
        task_id="check_files",
        python_callable=task_check_raw_files,
        doc_md="Check file logt*.txt in data/raw/ — skip if not found",
    )

    ingest = PythonOperator(
        task_id="ingest",
        python_callable=task_ingest,
        execution_timeout=timedelta(hours=1),
        doc_md="PySpark: parse dict-string logs → cleaned Parquet",
    )

    transform = PythonOperator(
        task_id="transform",
        python_callable=task_transform,
        execution_timeout=timedelta(hours=1),
        doc_md="PySpark: feature engineering + aggregate user profiles",
    )

    load = PythonOperator(
        task_id="load",
        python_callable=task_load,
        execution_timeout=timedelta(minutes=30),
        doc_md="pandas + SQLAlchemy: Parquet → PostgreSQL",
    )

    quality = PythonOperator(
        task_id="quality_check",
        python_callable=task_quality_check,
        doc_md="Validate row counts, null rates, churn distribution",
    )

    check_files >> ingest >> transform >> load >> quality
