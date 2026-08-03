from airflow import DAG
from datetime import datetime

default_args = {
    "owner": "Harshita",
    "depends_on_past": False,
    "retries": 2
}

with DAG(
    dag_id="ecommerce_data_pipeline",
    default_args=default_args,
    start_date=datetime(2026, 8, 1),
    schedule="@daily",
    catchup=False,
    tags=["production", "ecommerce"],
) as dag:

    pass