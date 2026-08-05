from datetime import datetime, timedelta

from airflow import DAG

from airflow.providers.amazon.aws.operators.lambda_function import LambdaInvokeFunctionOperator
from airflow.providers.amazon.aws.operators.glue import GlueJobOperator
from airflow.providers.amazon.aws.operators.glue_crawler import GlueCrawlerOperator
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator

default_args = {
    "owner": "Harshita",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
        dag_id="ecommerce_pipeline",
        default_args=default_args,
        description="Production Ecommerce Data Engineering Pipeline",
        start_date=datetime(2026, 8, 5),
        schedule="@daily",
        catchup=False,
        max_active_runs=1,
        tags=["ecommerce", "data-engineering", "aws"],
        dagrun_timeout=timedelta(hours=3),
) as dag:
    dag.doc_md = """
    # Ecommerce Data Engineering Pipeline

    ## Pipeline Flow

    RDS
        │
        ▼
    Lambda (RDS → Bronze)
        │
        ▼
    Bronze RDS Crawler
        │

    External Files
        │
        ▼
    Lambda (External Files → Bronze)
        │
        ▼
    Bronze Files Crawler
        │

    Both Bronze Crawlers Complete
        │
        ▼
    Glue Bronze → Silver
        │
        ▼
    Silver Crawler
        │
        ▼
    Validate Silver
        │
        ▼
    Snowflake Silver
        │
        ▼
    Glue Silver → Gold
        │
        ▼
    Validate Gold
        │
        ▼
    Snowflake Gold
    """

    # Bronze Layer - Lambda Tasks
    rds_to_bronze = LambdaInvokeFunctionOperator(
        task_id="rds_to_bronze",
        function_name="rds_to_bronze",
        aws_conn_id="aws_default",
        invocation_type="RequestResponse"
    )

    external_files_to_bronze = LambdaInvokeFunctionOperator(
        task_id="external_files_to_bronze",
        function_name="external_files_to_bronze",
        aws_conn_id="aws_default",
        invocation_type="RequestResponse"
    )

    # Bronze Crawlers
    bronze_rds_crawler = GlueCrawlerOperator(
        task_id="bronze_rds_crawler",
        config={
            "Name": "bronze_rds_crawler"
        },
        aws_conn_id="aws_default"
    )

    bronze_files_crawler = GlueCrawlerOperator(
        task_id="bronze_files_crawler",
        config={
            "Name": "bronze_files_crawler"
        },
        aws_conn_id="aws_default"
    )

    # Glue ETL Jobs
    bronze_to_silver = GlueJobOperator(
        task_id="bronze_to_silver",
        job_name="bronze_to_silver",
        aws_conn_id="aws_default",
        wait_for_completion=True
    )

    validate_silver = GlueJobOperator(
        task_id="validate_silver",
        job_name="validate_silver",
        aws_conn_id="aws_default",
        wait_for_completion=True
    )

    silver_to_gold = GlueJobOperator(
        task_id="silver_to_gold",
        job_name="silver_to_gold",
        aws_conn_id="aws_default",
        wait_for_completion=True
    )

    validate_gold = GlueJobOperator(
        task_id="validate_gold",
        job_name="validate_gold",
        aws_conn_id="aws_default",
        wait_for_completion=True
    )

    # Silver Crawler
    silver_crawler = GlueCrawlerOperator(
        task_id="silver_crawler",
        config={
            "Name": "silver_crawler"
        },
        aws_conn_id="aws_default"
    )

    # Snowflake Stored Procedures
    load_silver = SnowflakeOperator(
        task_id="load_silver",
        snowflake_conn_id="snowflake_default",
        sql="CALL SP_LOAD_SILVER();"
    )

    load_gold = SnowflakeOperator(
        task_id="load_gold",
        snowflake_conn_id="snowflake_default",
        sql="CALL SP_LOAD_GOLD();"
    )

    # ==========================================================
    # Task Dependencies
    # ==========================================================

    rds_to_bronze >> bronze_rds_crawler

    external_files_to_bronze >> bronze_files_crawler

    [bronze_rds_crawler, bronze_files_crawler] >> bronze_to_silver

    bronze_to_silver >> validate_silver

    validate_silver >> silver_crawler

    silver_crawler >> load_silver

    load_silver >> silver_to_gold

    silver_to_gold >> validate_gold

    validate_gold >> load_gold