import os
import json
from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))

CONF = os.path.join(BASE_DIR, "config", "config.json")
with open(CONF) as f:
    cfg = json.load(f)

def script(script_name):
    return os.path.join("etl", script_name)

default_args = {
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}

with DAG(
    dag_id="ecommerce_pipeline",
    start_date=datetime(2025, 1, 1),
    schedule_interval="@once",
    catchup=False,
    default_args=default_args,
    description="ETL pipeline for e-commerce data processing",
    tags=["etl"],
) as dag:

    bronze = BashOperator(
        task_id="bronze_to_silver",
        bash_command=f"cd {BASE_DIR} && spark-submit {script('bronze_to_silver.py')}",
    )

    silver = BashOperator(
        task_id="silver_to_gold",
        bash_command=f"cd {BASE_DIR} && spark-submit {script('silver_to_gold.py')}",
    )

    dq = BashOperator(
        task_id="data_quality",
        bash_command=f"cd {BASE_DIR} && spark-submit {script('data_quality.py')}",
    )

    load = BashOperator(
        task_id="load_postgres",
        bash_command=f"cd {BASE_DIR} && spark-submit {script('load_postgres.py')}",
    )

    bronze >> silver >> dq >> load