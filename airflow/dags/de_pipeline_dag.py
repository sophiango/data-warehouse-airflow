from __future__ import annotations
from pathlib import Path
from datetime import datetime, timedelta
import os, subprocess, sys

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

DAG_DIR = Path(__file__).resolve().parent
REPO_ROOT = DAG_DIR.parent.parent
GEN_SCRIPT = REPO_ROOT / "etl" / "generate_order.py"
LOAD_SCRIPT = REPO_ROOT / "etl" / "load_to_postgres.py"
WAREHOUSE_SQL = REPO_ROOT / "sql" / "build_warehouse.sql"
RUN_KPI = REPO_ROOT / "spark" / "kpi.py"

default_args = {"owner": "sophia", "retries": 1, "retry_delay": timedelta(minutes=2)}

def run_python_script(path: str):
    subprocess.check_call([sys.executable, str(path)], cwd=str(REPO_ROOT))

with DAG(
    dag_id="de_pipeline",
    description="End-to-end DE pipeline: generate -> load -> transform -> spark KPIs",
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,   # manual trigger first
    catchup=False,
    default_args=default_args,
    max_active_runs=1,
    template_searchpath=[str(REPO_ROOT / "sql")],
    tags=["data-engineering","demo"],
):
    generate_data = PythonOperator(
        task_id="generate_data",
        python_callable=run_python_script,
        op_kwargs={"path": str(GEN_SCRIPT)},
    )

    load_staging = PythonOperator(
        task_id="load_staging",
        python_callable=run_python_script,
        op_kwargs={"path": str(LOAD_SCRIPT)},
    )

    build_warehouse = PostgresOperator(
        task_id="build_warehouse",
        postgres_conn_id="postgres_de",
        sql="build_warehouse.sql"
    )

    spark_kpis = SparkSubmitOperator(
        task_id="spark_kpis",
        application="/Users/sophiango/src/data-warehouse-airflow/spark/kpi.py",
        conn_id="spark_default",
        packages="org.postgresql:postgresql:42.7.4",
        conf={"spark.master": "local[*]"},
        env_vars={
            "PYSPARK_PYTHON": "/Users/sophiango/src/data-warehouse-airflow/.airflowenv/bin/python3",
            "PYSPARK_DRIVER_PYTHON": "/Users/sophiango/src/data-warehouse-airflow/.airflowenv/bin/python3"
        }
    )

    generate_data >> load_staging >> build_warehouse >> spark_kpis