import sys

from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.sql.functions import count,col
from pyspark.sql.window import Window
from pyspark.sql import functions as F
from pyspark import StorageLevel


from common.logger import ETLLogger

from common.silver_utils import (
    remove_soft_deleted,
    remove_duplicates,
    trim_string_columns,
    handle_null_values,
    standardize_values
)
import uuid
from datetime import datetime

# Initialize Spark and Glue Context
args = getResolvedOptions(sys.argv, ['JOB_NAME'])


RUN_ID = f"{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}_{uuid.uuid4().hex[:8]}"

SILVER_CHANGE_LOG_PATH = (
    "s3://e-commerce-de-project/metadata/silver_change_log/"
)

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

job = Job(glueContext)
job.init(args['JOB_NAME'], args)

SILVER_BASE_PATH = "s3://e-commerce-de-project/silver"

def read_bronze_incremental(table_name, transformation_ctx):
    """
    Reads only new Bronze S3 objects using AWS Glue Job Bookmarks.

    First successful run:
        Reads all existing Bronze files.

    Subsequent runs:
        Reads only new Bronze files since the last successful run.
    """

    dynamic_frame = glueContext.create_dynamic_frame.from_catalog(
        database="bronze_db",
        table_name=table_name,
        transformation_ctx=transformation_ctx
    )

    df = dynamic_frame.toDF()

    logger = ETLLogger(
        job_name=args["JOB_NAME"],
        layer="silver",
        table_name=table_name
    )

    logger.info(
        event="READ_BRONZE_INCREMENTAL",
        message=f"Reading incremental Bronze data for {table_name}",
        rows_read=df.count()
    )

    return df

def record_change_manifest(
    changes_df,
    table_name,
    primary_keys
):
    """
    Records the business keys changed during this Silver run.

    This manifest is consumed by Gold so Gold does not need
    to scan the complete Silver table to discover changes.
    """

    if changes_df.rdd.isEmpty():
        return

    try:

        manifest_df = changes_df.select(
            *primary_keys
        ).dropDuplicates()

        # ------------------------------------------------------
        # Determine operation
        # ------------------------------------------------------

        if "is_deleted" in changes_df.columns:

            deleted_keys = (
                changes_df
                .filter(F.col("is_deleted") == 1)
                .select(*primary_keys)
                .dropDuplicates()
                .withColumn(
                    "operation",
                    F.lit("DELETE")
                )
            )

            active_keys = (
                changes_df
                .filter(
                    F.col("is_deleted").isNull()
                    | (F.col("is_deleted") != 1)
                )
                .select(*primary_keys)
                .dropDuplicates()
                .withColumn(
                    "operation",
                    F.lit("UPSERT")
                )
            )

            manifest_df = active_keys.unionByName(
                deleted_keys,
                allowMissingColumns=True
            )

        else:

            manifest_df = manifest_df.withColumn(
                "operation",
                F.lit("UPSERT")
            )

        # ------------------------------------------------------
        # Technical metadata
        # ------------------------------------------------------

        manifest_df = (
            manifest_df
            .withColumn(
                "table_name",
                F.lit(table_name)
            )
            .withColumn(
                "silver_run_id",
                F.lit(RUN_ID)
            )
            .withColumn(
                "changed_at",
                F.current_timestamp()
            )
            .withColumn(
                "change_date",
                F.current_date()
            )
        )

        # ------------------------------------------------------
        # Write change manifest
        # ------------------------------------------------------

        (
            manifest_df
            .write
            .mode("append")
            .partitionBy(
                "change_date",
                "table_name"
            )
            .parquet(
                SILVER_CHANGE_LOG_PATH
            )
        )

        print(
            f"Change manifest written for {table_name}. "
            f"Run ID = {RUN_ID}"
        )

    except Exception as e:

        print(
            f"Failed to write change manifest "
            f"for {table_name}: {str(e)}"
        )

        raise

def upsert_silver(
    changes_df,
    table_name,
    primary_keys,
    deleted_keys_df=None,
    order_column="updated_at"
):
    """
    Production-style incremental Silver upsert.

    Supports:

    1. Initial full load
    2. Incremental inserts
    3. Incremental updates
    4. Soft deletes
    5. Composite primary keys
    6. Hash-bucket based physical partition pruning
    7. Change manifest generation

    Silver remains a current-state dataset.
    """

    try:

        silver_path = f"{SILVER_BASE_PATH}/{table_name}/"

        # ------------------------------------------------------
        # Normalize primary keys
        # ------------------------------------------------------

        if isinstance(primary_keys, str):
            primary_keys = [primary_keys]

        logger = ETLLogger(
            job_name=args["JOB_NAME"],
            layer="silver",
            table_name=table_name
        )

        # ------------------------------------------------------
        # Validate primary keys
        # ------------------------------------------------------

        missing_keys = [
            key
            for key in primary_keys
            if key not in changes_df.columns
        ]

        if missing_keys:

            raise ValueError(
                f"Missing primary key columns for {table_name}: "
                f"{missing_keys}"
            )

        # ------------------------------------------------------
        # Deduplicate incoming batch
        # ------------------------------------------------------

        if order_column and order_column in changes_df.columns:

            window_spec = (
                Window
                .partitionBy(*primary_keys)
                .orderBy(
                    F.col(order_column)
                    .desc_nulls_last()
                )
            )

            changes_df = (
                changes_df
                .withColumn(
                    "_row_number",
                    F.row_number().over(window_spec)
                )
                .filter(
                    F.col("_row_number") == 1
                )
                .drop("_row_number")
            )

        else:

            changes_df = (
                changes_df
                .dropDuplicates(primary_keys)
            )

        # ------------------------------------------------------
        # Add technical Silver metadata
        # ------------------------------------------------------

        changes_df = (
            changes_df
            .withColumn(
                "_silver_run_id",
                F.lit(RUN_ID)
            )
            .withColumn(
                "_silver_processed_at",
                F.current_timestamp()
            )
        )

        # ------------------------------------------------------
        # Create deterministic hash bucket
        # ------------------------------------------------------

        bucket_expression = (
            F.pmod(
                F.abs(
                    F.hash(
                        *[
                            F.col(key).cast("string")
                            for key in primary_keys
                        ]
                    )
                ),
                F.lit(32)
            )
        )

        changes_df = changes_df.withColumn(
            "_bucket",
            bucket_expression
        )

        # ------------------------------------------------------
        # Record change manifest BEFORE writing Silver
        # ------------------------------------------------------



        # ======================================================
        # INITIAL LOAD
        # ======================================================

        hadoop_conf = spark._jsc.hadoopConfiguration()

        silver_path_uri = spark._jvm.java.net.URI.create(silver_path)

        fs = (
            spark._jvm
            .org.apache.hadoop.fs.FileSystem
            .get(
                silver_path_uri,
                hadoop_conf
            )
        )

        silver_path_obj = (
            spark._jvm.org.apache.hadoop.fs.Path(
                silver_path
            )
        )

        silver_exists = fs.exists(silver_path_obj)

        if not silver_exists:
            logger.info(
                event="FULL_LOAD",
                message=(
                    f"Silver table {table_name} "
                    "does not exist. Performing initial full load."
                )
            )

            (
                changes_df
                .write
                .mode("overwrite")
                .partitionBy("_bucket")
                .parquet(silver_path)
            )

            logger.info(
                event="FULL_LOAD_SUCCESS",
                message=(
                    f"Initial Silver load completed "
                    f"for {table_name}"
                )
            )

            # ------------------------------------------------------
            # Record initial load in Silver change manifest
            # ------------------------------------------------------
            record_change_manifest(
                changes_df=changes_df,
                table_name=table_name,
                primary_keys=primary_keys
            )

            return

        # ======================================================
        # INCREMENTAL LOAD
        # ======================================================

        logger.info(
            event="INCREMENTAL_LOAD",
            message=(
                f"Processing incremental Silver load "
                f"for {table_name}"
            )
        )

        # ------------------------------------------------------
        # Find affected buckets
        # ------------------------------------------------------

        affected_buckets = [
            row["_bucket"]
            for row in (
                changes_df
                .select("_bucket")
                .distinct()
                .collect()
            )
        ]

        logger.info(
            event="AFFECTED_BUCKETS",
            message=(
                f"Affected buckets for {table_name}: "
                f"{affected_buckets}"
            )
        )

        if not affected_buckets:
            logger.info(
                event="NO_AFFECTED_BUCKETS",
                message=(
                    f"No affected buckets for {table_name}"
                )
            )

            return

        # ------------------------------------------------------
        # Read ONLY affected Silver partitions
        # ------------------------------------------------------

        existing_df = (
            spark
            .read
            .parquet(silver_path)
            .filter(
                F.col("_bucket").isin(
                    affected_buckets
                )
            )
            .persist(StorageLevel.MEMORY_AND_DISK)
        )

        # Force Spark to materialize and cache the affected Silver data
        existing_count = existing_df.count()

        logger.info(
            event="READ_AFFECTED_SILVER",
            message=(
                f"Read only affected Silver partitions "
                f"for {table_name}"
            ),
            rows_read=existing_count
        )

        # ------------------------------------------------------
        # Identify changed keys
        # ------------------------------------------------------

        changed_keys_df = (
            changes_df
            .select(
                *primary_keys
            )
            .dropDuplicates()
        )

        # ------------------------------------------------------
        # Remove old versions ONLY for changed keys
        # ------------------------------------------------------

        unchanged_df = (
            existing_df
            .join(
                changed_keys_df,
                primary_keys,
                "left_anti"
            )
        )

        # ------------------------------------------------------
        # Add incoming latest records
        # ------------------------------------------------------

        final_affected_df = (
            unchanged_df
            .unionByName(
                changes_df,
                allowMissingColumns=True
            )
        )

        # ------------------------------------------------------
        # Explicit deleted keys
        # ------------------------------------------------------

        if deleted_keys_df is not None:

            deleted_keys_df = (
                deleted_keys_df
                .select(*primary_keys)
                .dropDuplicates()
            )

            final_affected_df = (
                final_affected_df
                .join(
                    deleted_keys_df,
                    primary_keys,
                    "left_anti"
                )
            )

        # ------------------------------------------------------
        # Dynamic partition overwrite
        # ------------------------------------------------------

        spark.conf.set(
            "spark.sql.sources.partitionOverwriteMode",
            "dynamic"
        )

        # Materialize final result BEFORE overwriting Silver
        rows_to_write = final_affected_df.count()

        (
            final_affected_df
            .write
            .mode("overwrite")
            .partitionBy("_bucket")
            .parquet(silver_path)
        )

        # Record change manifest ONLY after Silver write succeeds
        record_change_manifest(
            changes_df=changes_df,
            table_name=table_name,
            primary_keys=primary_keys
        )

        logger.info(
            event="SILVER_INCREMENTAL_SUCCESS",
            message=(
                f"Silver {table_name} incrementally "
                "updated successfully."
            ),
            rows_written=rows_to_write
        )

        # Release cached Silver data
        existing_df.unpersist()

    except Exception as e:

        logger.error(
            event="SILVER_UPSERT_FAILED",
            message=(
                f"Silver upsert failed for {table_name}: "
                f"{str(e)}"
            )
        )

        raise

def process_customers():

    logger = ETLLogger(
        job_name=args["JOB_NAME"],
        layer="silver",
        table_name="customers"
    )

    try:
        # start_time = time.time()

        logger.info(
            event="JOB_START",
            message="Customers Bronze to Silver ETL started"
        )

        customers_df = read_bronze_incremental(
            "customers",
            "customers_bronze"
        )

        if customers_df.rdd.isEmpty():
            logger.info(
                event="NO_NEW_DATA",
                message="No new Bronze data for customers"
            )
            return
        logger.info(
            event="READ_BRONZE",
            message="Reading customers table from Bronze Catalog"
        )

        logger.info(
            event="TRANSFORM",
            message="Applying Silver transformations"
        )
        rows_read = customers_df.count()

        logger.info(
            event="ROWS_READ",
            message="Initial row count",
            rows_read=rows_read
        )

        # Soft Delete Check
        # customers_df = remove_soft_deleted(customers_df)
        # count_after_soft_delete = customers_df.count()
        #
        # logger.info(
        #     event="REMOVE_SOFT_DELETED",
        #     message="Soft deleted records removed",
        #     rows_after_soft_delete=count_after_soft_delete
        # )


        #Remove duplicates
        customers_df = remove_duplicates(
            customers_df,
            "customer_id"
        )
        after_duplicates_count = customers_df.count()

        logger.info(
            event="REMOVE_DUPLICATES",
            message="Duplicate removal completed",
            rows_after_duplicates=after_duplicates_count
        )


        customers_df = trim_string_columns(customers_df)

        after_trim_count = customers_df.count()

        logger.info(
            event="TRIM_STRING_COLUMNS",
            message="String trimming completed",
            rows_after_trim=after_trim_count
        )

        customers_df = handle_null_values(customers_df)

        logger.info(
            event="HANDLE_NULL_VALUES",
            message="NULL value handling completed"
        )

        customers_df = standardize_values(customers_df)

        logger.info(
            event="STANDARDIZE_VALUES",
            message="Value standardization completed"
        )

        logger.info(
            event="WRITE_SILVER",
            message="Writing customers table to Silver"
        )
        upsert_silver(
            changes_df=customers_df,
            table_name="customers",
            primary_keys=["customer_id"],
            order_column="updated_at"
        )

        logger.info(
            event="JOB_SUCCESS",
            message="Bronze to Silver ETL completed successfully"
        )

    except Exception as e:

        logger.error(
            event="JOB_FAILED",
            message=str(e)
        )

        raise

    finally:

        logger.info(
            event="JOB_END",
            message="Bronze to Silver ETL finished"
        )


def process_orders():

    logger = ETLLogger(
        job_name=args["JOB_NAME"],
        layer="silver",
        table_name="orders"
    )

    try:

        logger.info(
            event="JOB_START",
            message="Orders Bronze to Silver ETL started"
        )

        orders_df = read_bronze_incremental(
            "orders",
            "orders_bronze"
        )

        if orders_df.rdd.isEmpty():
            logger.info(
                event="NO_NEW_DATA",
                message="No new Bronze data for orders"
            )
            return

        orders_df.printSchema()
        orders_df.select("order_purchase_timestamp").show(5, False)

        logger.info(
            event="READ_BRONZE",
            message="Reading orders table from Bronze Catalog"
        )

        logger.info(
            event="TRANSFORM",
            message="Applying Silver transformations"
        )

        rows_read = orders_df.count()

        logger.info(
            event="ROWS_READ",
            message="Initial row count",
            rows_read=rows_read
        )

        # =====================================================
        # REMOVE SOFT DELETED RECORDS
        # =====================================================

        # orders_df = remove_soft_deleted(orders_df)
        #
        # count_after_soft_delete = orders_df.count()
        #
        # logger.info(
        #     event="REMOVE_SOFT_DELETED",
        #     message="Soft deleted records removed",
        #     rows_after_soft_delete=count_after_soft_delete
        # )

        # Remove duplicates
        orders_df = remove_duplicates(
            orders_df,
            "order_id"
        )

        after_duplicates_count = orders_df.count()

        logger.info(
            event="REMOVE_DUPLICATES",
            message="Duplicate removal completed",
            rows_after_duplicates=after_duplicates_count
        )


        orders_df = trim_string_columns(orders_df)

        after_trim_count = orders_df.count()

        logger.info(
            event="TRIM_STRING_COLUMNS",
            message="String trimming completed",
            rows_after_trim=after_trim_count
        )

        orders_df = standardize_values(orders_df)

        logger.info(
            event="STANDARDIZE_VALUES",
            message="Value standardization completed"
        )

        logger.info(
            event="WRITE_SILVER",
            message="Writing orders table to Silver"
        )

        upsert_silver(
            changes_df=orders_df,
            table_name="orders",
            primary_keys=["order_id"],
            order_column="updated_at"
        )

        logger.info(
            event="JOB_SUCCESS",
            message="Bronze to Silver ETL completed successfully"
        )

    except Exception as e:

        logger.error(
            event="JOB_FAILED",
            message=str(e)
        )

        raise

    finally:

        logger.info(
            event="JOB_END",
            message="Bronze to Silver ETL finished"
        )

def process_order_items():

    logger = ETLLogger(
        job_name=args["JOB_NAME"],
        layer="silver",
        table_name="order_items"
    )

    try:

        logger.info(
            event="JOB_START",
            message="Order Items Bronze to Silver ETL started"
        )

        order_items_df = read_bronze_incremental(
            "order_items",
            "order_items_bronze"
        )

        if order_items_df.rdd.isEmpty():
            logger.info(
                event="NO_NEW_DATA",
                message="No new Bronze data for order_items"
            )
            return

        logger.info(
            event="READ_BRONZE",
            message="Reading order_items table from Bronze Catalog"
        )

        logger.info(
            event="TRANSFORM",
            message="Applying Silver transformations"
        )

        rows_read = order_items_df.count()

        logger.info(
            event="ROWS_READ",
            message="Initial row count",
            rows_read=rows_read
        )

        # -----------------------------------------
        # Remove Soft Deleted Records
        # -----------------------------------------

        # order_items_df = remove_soft_deleted(order_items_df)
        # count_after_soft_delete = order_items_df.count()
        #
        # logger.info(
        #     event="REMOVE_SOFT_DELETED",
        #     message="Soft deleted records removed",
        #     rows_after_soft_delete=count_after_soft_delete
        # )

        # -----------------------------------------
        # Remove Duplicates
        # -----------------------------------------

        order_items_df = remove_duplicates(
            order_items_df,
            ["order_id", "order_item_id"]
        )

        after_duplicates_count = order_items_df.count()

        logger.info(
            event="REMOVE_DUPLICATES",
            message="Duplicate removal completed",
            rows_after_duplicates=after_duplicates_count
        )
        # -----------------------------------------
        # Trim String Columns
        # -----------------------------------------

        order_items_df = trim_string_columns(order_items_df)
        after_trim_count = order_items_df.count()

        logger.info(
            event="TRIM_STRING_COLUMNS",
            message="String trimming completed",
            rows_after_trim=after_trim_count
        )

        # -----------------------------------------
        # Handle NULL Values
        # -----------------------------------------

        order_items_df = handle_null_values(order_items_df)
        logger.info(
            event="HANDLE_NULL_VALUES",
            message="NULL value handling completed"
        )

        # -----------------------------------------
        # Standardize Values
        # -----------------------------------------

        order_items_df = standardize_values(order_items_df)
        logger.info(
            event="STANDARDIZE_VALUES",
            message="Value standardization completed"
        )

        logger.info(
            event="WRITE_SILVER",
            message="Writing order_items table to Silver"
        )

        upsert_silver(
            changes_df=order_items_df,
            table_name="order_items",
            primary_keys=["order_id", "order_item_id"],
            order_column="updated_at"
        )

        logger.info(
            event="JOB_SUCCESS",
            message="Order Items Bronze to Silver ETL completed successfully"
        )

    except Exception as e:

        logger.error(
            event="JOB_FAILED",
            message=str(e)
        )

        raise

    finally:

        logger.info(
            event="JOB_END",
            message="Order Items Bronze to Silver ETL finished"
        )

def process_products():

    logger = ETLLogger(
        job_name=args["JOB_NAME"],
        layer="silver",
        table_name="products"
    )

    try:

        logger.info(
            event="JOB_START",
            message="Products Bronze to Silver ETL started"
        )

        # ---------------------------------------------------
        # Read Bronze Products
        # ---------------------------------------------------

        products_df = read_bronze_incremental(
            "products",
            "products_bronze"
        )

        if products_df.rdd.isEmpty():
            logger.info(
                event="NO_NEW_DATA",
                message="No new Bronze data for products"
            )
            return

        logger.info(
            event="READ_BRONZE",
            message="Reading products table from Bronze Catalog"
        )

        logger.info(
            event="TRANSFORM",
            message="Applying Silver transformations"
        )

        rows_read = products_df.count()

        logger.info(
            event="ROWS_READ",
            message="Initial row count",
            rows_read=rows_read
        )

        # ---------------------------------------------------
        # Remove Soft Deleted Records
        # ---------------------------------------------------

        # products_df = remove_soft_deleted(products_df)
        #
        # count_after_soft_delete = products_df.count()
        #
        # logger.info(
        #     event="REMOVE_SOFT_DELETED",
        #     message="Soft deleted records removed",
        #     rows_after_soft_delete=count_after_soft_delete
        # )

        # ---------------------------------------------------
        # Remove Duplicates
        # ---------------------------------------------------

        products_df = remove_duplicates(
            products_df,
            "product_id"
        )

        after_duplicates_count = products_df.count()

        logger.info(
            event="REMOVE_DUPLICATES",
            message="Duplicate removal completed",
            rows_after_duplicates=after_duplicates_count
        )

        # ---------------------------------------------------
        # Trim String Columns
        # ---------------------------------------------------

        products_df = trim_string_columns(products_df)

        after_trim_count = products_df.count()

        logger.info(
            event="TRIM_STRING_COLUMNS",
            message="String trimming completed",
            rows_after_trim=after_trim_count
        )

        # ---------------------------------------------------
        # Handle NULL Values
        # ---------------------------------------------------

        products_df = handle_null_values(products_df)

        logger.info(
            event="HANDLE_NULL_VALUES",
            message="NULL value handling completed"
        )

        # ---------------------------------------------------
        # Standardize Values
        # ---------------------------------------------------

        products_df = standardize_values(products_df)

        logger.info(
            event="STANDARDIZE_VALUES",
            message="Value standardization completed"
        )

        # ---------------------------------------------------
        # Write Silver
        # ---------------------------------------------------

        logger.info(
            event="WRITE_SILVER",
            message="Writing products table to Silver"
        )

        upsert_silver(
            changes_df=products_df,
            table_name="products",
            primary_keys=["product_id"],
            order_column="updated_at"
        )

        logger.info(
            event="JOB_SUCCESS",
            message="Products Bronze to Silver ETL completed successfully"
        )

    except Exception as e:

        logger.error(
            event="JOB_FAILED",
            message=str(e)
        )

        raise

    finally:

        logger.info(
            event="JOB_END",
            message="Products Bronze to Silver ETL finished"
        )

def process_payments():

    logger = ETLLogger(
        job_name=args["JOB_NAME"],
        layer="silver",
        table_name="payments"
    )

    try:

        logger.info(
            event="JOB_START",
            message="Payments Bronze to Silver ETL started"
        )

        # ---------------------------------------------------
        # Read Bronze Payments
        # ---------------------------------------------------

        payments_df = read_bronze_incremental(
            "payments",
            "payments_bronze"
        )

        if payments_df.rdd.isEmpty():
            logger.info(
                event="NO_NEW_DATA",
                message="No new Bronze data for payments"
            )
            return

        logger.info(
            event="READ_BRONZE",
            message="Reading payments table from Bronze Catalog"
        )

        logger.info(
            event="TRANSFORM",
            message="Applying Silver transformations"
        )

        rows_read = payments_df.count()

        logger.info(
            event="ROWS_READ",
            message="Initial row count",
            rows_read=rows_read
        )

        # ---------------------------------------------------
        # Remove Soft Deleted Records
        # ---------------------------------------------------

        # payments_df = remove_soft_deleted(payments_df)
        #
        # count_after_soft_delete = payments_df.count()
        #
        # logger.info(
        #     event="REMOVE_SOFT_DELETED",
        #     message="Soft deleted records removed",
        #     rows_after_soft_delete=count_after_soft_delete
        # )

        # ---------------------------------------------------
        # Remove Duplicates
        # ---------------------------------------------------

        payments_df = remove_duplicates(
            payments_df,
            ["order_id", "payment_sequential"]
        )

        after_duplicates_count = payments_df.count()

        logger.info(
            event="REMOVE_DUPLICATES",
            message="Duplicate removal completed",
            rows_after_duplicates=after_duplicates_count
        )

        # ---------------------------------------------------
        # Trim String Columns
        # ---------------------------------------------------

        payments_df = trim_string_columns(payments_df)

        after_trim_count = payments_df.count()

        logger.info(
            event="TRIM_STRING_COLUMNS",
            message="String trimming completed",
            rows_after_trim=after_trim_count
        )

        # ---------------------------------------------------
        # Handle NULL Values
        # ---------------------------------------------------

        payments_df = handle_null_values(payments_df)

        logger.info(
            event="HANDLE_NULL_VALUES",
            message="NULL value handling completed"
        )

        # ---------------------------------------------------
        # Standardize Values
        # ---------------------------------------------------

        payments_df = standardize_values(payments_df)

        logger.info(
            event="STANDARDIZE_VALUES",
            message="Value standardization completed"
        )

        # ---------------------------------------------------
        # Write Silver
        # ---------------------------------------------------

        logger.info(
            event="WRITE_SILVER",
            message="Writing payments table to Silver"
        )

        upsert_silver(
            changes_df=payments_df,
            table_name="payments",
            primary_keys=["order_id", "payment_sequential"],
            order_column="updated_at"
        )

        logger.info(
            event="JOB_SUCCESS",
            message="Payments Bronze to Silver ETL completed successfully"
        )

    except Exception as e:

        logger.error(
            event="JOB_FAILED",
            message=str(e)
        )

        raise

    finally:

        logger.info(
            event="JOB_END",
            message="Payments Bronze to Silver ETL finished"
        )

def process_sellers():

    logger = ETLLogger(
        job_name=args["JOB_NAME"],
        layer="silver",
        table_name="sellers"
    )

    try:

        logger.info(
            event="JOB_START",
            message="Sellers Bronze to Silver ETL started"
        )

        # ---------------------------------------------------
        # Read Bronze Sellers
        # ---------------------------------------------------

        sellers_df = read_bronze_incremental(
            "sellers",
            "sellers_bronze"
        )

        if sellers_df.rdd.isEmpty():
            logger.info(
                event="NO_NEW_DATA",
                message="No new Bronze data for sellers"
            )
            return

        logger.info(
            event="READ_BRONZE",
            message="Reading sellers table from Bronze Catalog"
        )

        logger.info(
            event="TRANSFORM",
            message="Applying Silver transformations"
        )

        rows_read = sellers_df.count()

        logger.info(
            event="ROWS_READ",
            message="Initial row count",
            rows_read=rows_read
        )

        # ---------------------------------------------------
        # Remove Soft Deleted Records
        # ---------------------------------------------------

        # sellers_df = remove_soft_deleted(sellers_df)
        #
        # count_after_soft_delete = sellers_df.count()
        #
        # logger.info(
        #     event="REMOVE_SOFT_DELETED",
        #     message="Soft deleted records removed",
        #     rows_after_soft_delete=count_after_soft_delete
        # )

        # ---------------------------------------------------
        # Remove Duplicates
        # ---------------------------------------------------

        sellers_df = remove_duplicates(
            sellers_df,
            "seller_id"
        )

        after_duplicates_count = sellers_df.count()

        logger.info(
            event="REMOVE_DUPLICATES",
            message="Duplicate removal completed",
            rows_after_duplicates=after_duplicates_count
        )

        # ---------------------------------------------------
        # Trim String Columns
        # ---------------------------------------------------

        sellers_df = trim_string_columns(sellers_df)

        after_trim_count = sellers_df.count()

        logger.info(
            event="TRIM_STRING_COLUMNS",
            message="String trimming completed",
            rows_after_trim=after_trim_count
        )

        # ---------------------------------------------------
        # Handle NULL Values
        # ---------------------------------------------------

        sellers_df = handle_null_values(sellers_df)

        logger.info(
            event="HANDLE_NULL_VALUES",
            message="NULL value handling completed"
        )

        # ---------------------------------------------------
        # Standardize Values
        # ---------------------------------------------------

        sellers_df = standardize_values(sellers_df)

        logger.info(
            event="STANDARDIZE_VALUES",
            message="Value standardization completed"
        )

        # ---------------------------------------------------
        # Write Silver
        # ---------------------------------------------------

        logger.info(
            event="WRITE_SILVER",
            message="Writing sellers table to Silver"
        )

        upsert_silver(
            changes_df=sellers_df,
            table_name="sellers",
            primary_keys=["seller_id"],
            order_column="updated_at"
        )

        logger.info(
            event="JOB_SUCCESS",
            message="Sellers Bronze to Silver ETL completed successfully"
        )

    except Exception as e:

        logger.error(
            event="JOB_FAILED",
            message=str(e)
        )

        raise

    finally:

        logger.info(
            event="JOB_END",
            message="Sellers Bronze to Silver ETL finished"
        )

def process_reviews():

    logger = ETLLogger(
        job_name=args["JOB_NAME"],
        layer="silver",
        table_name="reviews"
    )

    try:

        logger.info(
            event="JOB_START",
            message="Reviews Bronze to Silver ETL started"
        )

        # ---------------------------------------------------
        # Read Bronze Reviews
        # ---------------------------------------------------

        reviews_df = read_bronze_incremental(
            "reviews",
            "reviews_bronze"
        )

        if reviews_df.rdd.isEmpty():
            logger.info(
                event="NO_NEW_DATA",
                message="No new Bronze data for reviews"
            )
            return

        logger.info(
            event="READ_BRONZE",
            message="Reading reviews table from Bronze Catalog"
        )

        logger.info(
            event="TRANSFORM",
            message="Applying Silver transformations"
        )

        rows_read = reviews_df.count()

        logger.info(
            event="ROWS_READ",
            message="Initial row count",
            rows_read=rows_read
        )

        # ---------------------------------------------------
        # Remove Soft Deleted Records
        # ---------------------------------------------------

        # reviews_df = remove_soft_deleted(reviews_df)
        #
        # count_after_soft_delete = reviews_df.count()
        #
        # logger.info(
        #     event="REMOVE_SOFT_DELETED",
        #     message="Soft deleted records removed",
        #     rows_after_soft_delete=count_after_soft_delete
        # )

        # ---------------------------------------------------
        # Remove Duplicate Business Records
        # ---------------------------------------------------

        reviews_df = remove_duplicates(
            reviews_df,
            ["review_id", "order_id"]
        )

        after_duplicates_count = reviews_df.count()

        logger.info(
            event="REMOVE_DUPLICATES",
            message="Duplicate removal completed",
            rows_after_duplicates=after_duplicates_count
        )

        # ---------------------------------------------------
        # Trim String Columns
        # ---------------------------------------------------

        reviews_df = trim_string_columns(reviews_df)

        after_trim_count = reviews_df.count()

        logger.info(
            event="TRIM_STRING_COLUMNS",
            message="String trimming completed",
            rows_after_trim=after_trim_count
        )

        # ---------------------------------------------------
        # Handle NULL Values
        # ---------------------------------------------------

        reviews_df = handle_null_values(reviews_df)

        logger.info(
            event="HANDLE_NULL_VALUES",
            message="NULL value handling completed"
        )

        # ---------------------------------------------------
        # Write Silver
        # ---------------------------------------------------

        logger.info(
            event="WRITE_SILVER",
            message="Writing reviews table to Silver"
        )

        upsert_silver(
            changes_df=reviews_df,
            table_name="reviews",
            primary_keys=["review_id", "order_id"],
            order_column="updated_at"
        )

        logger.info(
            event="JOB_SUCCESS",
            message="Reviews Bronze to Silver ETL completed successfully"
        )

    except Exception as e:

        logger.error(
            event="JOB_FAILED",
            message=str(e)
        )

        raise

    finally:

        logger.info(
            event="JOB_END",
            message="Reviews Bronze to Silver ETL finished"
        )

def process_inventory():

    logger = ETLLogger(
        job_name=args["JOB_NAME"],
        layer="silver",
        table_name="inventory_initial_production_v2"
    )

    try:

        logger.info(
            event="JOB_START",
            message="Inventory Bronze to Silver ETL started"
        )

        # ---------------------------------------------------
        # Read Bronze Inventory
        # ---------------------------------------------------

        inventory_df = read_bronze_incremental(
            "inventory_initial_production_v2",
            "inventory_bronze"
        )

        if inventory_df.rdd.isEmpty():
            logger.info(
                event="NO_NEW_DATA",
                message="No new Bronze data for inventory"
            )
            return

        logger.info(
            event="READ_BRONZE",
            message="Reading inventory table from Bronze Catalog"
        )

        logger.info(
            event="TRANSFORM",
            message="Applying Silver transformations"
        )

        rows_read = inventory_df.count()

        logger.info(
            event="ROWS_READ",
            message="Initial row count",
            rows_read=rows_read
        )

        # ---------------------------------------------------
        # Remove Soft Deleted Records
        # ---------------------------------------------------

        # inventory_df = remove_soft_deleted(inventory_df)
        #
        # count_after_soft_delete = inventory_df.count()
        #
        # logger.info(
        #     event="REMOVE_SOFT_DELETED",
        #     message="Soft deleted records removed",
        #     rows_after_soft_delete=count_after_soft_delete
        # )

        # ---------------------------------------------------
        # Remove Duplicate Records
        # ---------------------------------------------------

        inventory_df = remove_duplicates(
            inventory_df,
            "inventory_id",
            order_column="last_updated"
        )

        after_duplicates_count = inventory_df.count()

        logger.info(
            event="REMOVE_DUPLICATES",
            message="Duplicate removal completed",
            rows_after_duplicates=after_duplicates_count
        )

        # ---------------------------------------------------
        # Trim String Columns
        # ---------------------------------------------------

        inventory_df = trim_string_columns(inventory_df)

        after_trim_count = inventory_df.count()

        logger.info(
            event="TRIM_STRING_COLUMNS",
            message="String trimming completed",
            rows_after_trim=after_trim_count
        )

        # ---------------------------------------------------
        # Handle NULL Values
        # ---------------------------------------------------

        inventory_df = handle_null_values(inventory_df)

        logger.info(
            event="HANDLE_NULL_VALUES",
            message="NULL value handling completed"
        )

        # ---------------------------------------------------
        # Standardize Values
        # ---------------------------------------------------

        inventory_df = standardize_values(inventory_df)

        logger.info(
            event="STANDARDIZE_VALUES",
            message="Value standardization completed"
        )

        # ---------------------------------------------------
        # Write Silver
        # ---------------------------------------------------

        logger.info(
            event="WRITE_SILVER",
            message="Writing inventory table to Silver"
        )

        upsert_silver(
            changes_df=inventory_df,
            table_name="inventory",
            primary_keys=["inventory_id"],
            order_column="last_updated"
        )

        logger.info(
            event="JOB_SUCCESS",
            message="Inventory Bronze to Silver ETL completed successfully"
        )

    except Exception as e:

        logger.error(
            event="JOB_FAILED",
            message=str(e)
        )

        raise

    finally:

        logger.info(
            event="JOB_END",
            message="Inventory Bronze to Silver ETL finished"
        )

def process_shipment():

    logger = ETLLogger(
        job_name=args["JOB_NAME"],
        layer="silver",
        table_name="shipment"
    )

    try:
        logger.info(
            event="JOB_START",
            message="Shipment Bronze to Silver ETL started"
        )

        shipment_df = read_bronze_incremental(
            "shipment_management_system",
            "shipment_bronze"
        )

        if shipment_df.rdd.isEmpty():
            logger.info(
                event="NO_NEW_DATA",
                message="No new Bronze data for shipment"
            )
            return

        logger.info(
            event="READ_BRONZE",
            message="Reading shipment table from Bronze Catalog"
        )

        logger.info(
            event="TRANSFORM",
            message="Applying Silver transformations"
        )

        rows_read = shipment_df.count()

        logger.info(
            event="ROWS_READ",
            message="Initial row count",
            rows_read=rows_read
        )

        # shipment_df = remove_soft_deleted(shipment_df)
        #
        # count_after_soft_delete = shipment_df.count()
        #
        # logger.info(
        #     event="REMOVE_SOFT_DELETED",
        #     message="Soft deleted records removed",
        #     rows_after_soft_delete=count_after_soft_delete
        # )

        shipment_df = remove_duplicates(
            shipment_df,
            "shipment_id",
            order_column="shipped_timestamp"
        )

        after_duplicates_count = shipment_df.count()

        logger.info(
            event="REMOVE_DUPLICATES",
            message="Duplicate removal completed",
            rows_after_duplicates=after_duplicates_count
        )

        shipment_df = trim_string_columns(shipment_df)

        after_trim_count = shipment_df.count()

        logger.info(
            event="TRIM_STRING_COLUMNS",
            message="String trimming completed",
            rows_after_trim=after_trim_count
        )

        shipment_df = standardize_values(shipment_df)

        logger.info(
            event="STANDARDIZE_VALUES",
            message="Value standardization completed"
        )

        logger.info(
            event="WRITE_SILVER",
            message="Writing shipment table to Silver"
        )

        upsert_silver(
            changes_df=shipment_df,
            table_name="shipment",
            primary_keys=["shipment_id"],
            order_column="shipped_timestamp"
        )

        logger.info(
            event="JOB_SUCCESS",
            message="Shipment Bronze to Silver ETL completed successfully"
        )

    except Exception as e:

        logger.error(
            event="JOB_FAILED",
            message=str(e)
        )

        raise

    finally:

        logger.info(
            event="JOB_END",
            message="Shipment Bronze to Silver ETL finished"
        )

def process_geolocation():

    logger = ETLLogger(
        job_name=args["JOB_NAME"],
        layer="silver",
        table_name="geolocation"
    )

    try:

        logger.info(
            event="JOB_START",
            message="Geolocation Bronze to Silver ETL started"
        )

        # ---------------------------------------------------
        # Read Bronze Geolocation
        # ---------------------------------------------------

        geolocation_df = read_bronze_incremental(
            "olist_geolocation_dataset",
            "geolocation_bronze"
        )

        if geolocation_df.rdd.isEmpty():
            logger.info(
                event="NO_NEW_DATA",
                message="No new Bronze data for geolocation"
            )
            return

        logger.info(
            event="READ_BRONZE",
            message="Reading geolocation table from Bronze Catalog"
        )

        logger.info(
            event="TRANSFORM",
            message="Applying Silver transformations"
        )

        rows_read = geolocation_df.count()

        logger.info(
            event="ROWS_READ",
            message="Initial row count",
            rows_read=rows_read
        )

        # ---------------------------------------------------
        # Remove Soft Deleted Records
        # ---------------------------------------------------

        # geolocation_df = remove_soft_deleted(geolocation_df)
        #
        # count_after_soft_delete = geolocation_df.count()
        #
        # logger.info(
        #     event="REMOVE_SOFT_DELETED",
        #     message="Soft deleted records removed",
        #     rows_after_soft_delete=count_after_soft_delete
        # )

        # ---------------------------------------------------
        # Remove Duplicate Records
        # ---------------------------------------------------

        geolocation_df = remove_duplicates(
            geolocation_df,
            [
                "geolocation_zip_code_prefix",
                "geolocation_lat",
                "geolocation_lng"
            ],
            order_column=None
        )

        after_duplicates_count = geolocation_df.count()

        logger.info(
            event="REMOVE_DUPLICATES",
            message="Duplicate removal completed",
            rows_after_duplicates=after_duplicates_count
        )

        # ---------------------------------------------------
        # Trim String Columns
        # ---------------------------------------------------

        geolocation_df = trim_string_columns(
            geolocation_df
        )

        after_trim_count = geolocation_df.count()

        logger.info(
            event="TRIM_STRING_COLUMNS",
            message="String trimming completed",
            rows_after_trim=after_trim_count
        )

        # ---------------------------------------------------
        # Handle NULL Values
        # ---------------------------------------------------

        geolocation_df = handle_null_values(
            geolocation_df
        )

        logger.info(
            event="HANDLE_NULL_VALUES",
            message="NULL value handling completed"
        )

        # ---------------------------------------------------
        # Standardize Values
        # ---------------------------------------------------

        geolocation_df = standardize_values(
            geolocation_df
        )

        logger.info(
            event="STANDARDIZE_VALUES",
            message="Value standardization completed"
        )

        # ---------------------------------------------------
        # Write Silver
        # ---------------------------------------------------

        logger.info(
            event="WRITE_SILVER",
            message="Writing geolocation table to Silver"
        )

        upsert_silver(
            changes_df=geolocation_df,
            table_name="geolocation",
            primary_keys=[
                "geolocation_zip_code_prefix",
                "geolocation_lat",
                "geolocation_lng"
            ],
            order_column=None
        )

        logger.info(
            event="JOB_SUCCESS",
            message="Geolocation Bronze to Silver ETL completed successfully"
        )

    except Exception as e:

        logger.error(
            event="JOB_FAILED",
            message=str(e)
        )

        raise

    finally:

        logger.info(
            event="JOB_END",
            message="Geolocation Bronze to Silver ETL finished"
        )

def main():

    process_customers()

    process_orders()

    process_order_items()

    process_products()

    process_payments()

    process_sellers()

    process_reviews()

    process_inventory()

    process_shipment()

    process_geolocation()

    job.commit()


if __name__ == "__main__":
    main()