import sys
import logging
import json
import traceback
from datetime import datetime

from pyspark.context import SparkContext
from pyspark.sql import functions as F

from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from awsglue.dynamicframe import DynamicFrame

import boto3
from botocore.exceptions import ClientError

from urllib.parse import urlparse
from pyspark.sql.window import Window

# ----------------------------------
# Job Initialization
# ----------------------------------

args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
spark.conf.set(
    "spark.sql.sources.partitionOverwriteMode",
    "dynamic"
)

spark.conf.set(
    "spark.sql.parquet.mergeSchema",
    "true"
)

spark.conf.set(
    "spark.sql.hive.convertMetastoreParquet.mergeSchema",
    "true"
)

job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# ----------------------------------
# Logger
# ----------------------------------

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s"
)

logger = logging.getLogger(__name__)

logger.info("Starting Gold ETL")

# ==========================================================
# TEST GLUE DATA CATALOG
# ==========================================================

test_df = spark.table("silver_db.orders")

logger.info(
    f"Catalog orders row count = {test_df.count()}"
)

test_df.printSchema()

# ==========================================================
# GOLD LOAD CONFIGURATION
# ==========================================================

silver_db = "silver_db"
gold_database = "gold_db"

SILVER_CHANGE_LOG_PATH = (
    "s3://e-commerce-de-project/metadata/silver_change_log/"
)

gold_path = "s3://e-commerce-de-project/gold/"
# Gold table folder structure
GOLD_TABLE_PATHS = {
    "dim_customer": f"{gold_path}dimensions/dim_customer/",
    "dim_product": f"{gold_path}dimensions/dim_product/",
    "dim_seller": f"{gold_path}dimensions/dim_seller/",
    "dim_inventory": f"{gold_path}dimensions/dim_inventory/",

    "fact_sales": f"{gold_path}facts/fact_sales/",
    "fact_payments": f"{gold_path}facts/fact_payments/",

    "fact_sales_daily": f"{gold_path}marts/fact_sales_daily/",
    "inventory_summary": f"{gold_path}marts/inventory_summary/",
}

# ==========================================================
# GOLD CHANGE FEED
# Used by Snowflake for incremental MERGE
# ==========================================================

GOLD_CHANGE_PATH = (
    "s3://e-commerce-de-project/gold_changes/"
)

WATERMARK_BUCKET = "e-commerce-de-project"
WATERMARK_KEY = "metadata/gold_watermark.json"

s3_client = boto3.client("s3")


# ==========================================================
# GET GOLD WATERMARK
# ==========================================================

def get_gold_watermark():

    try:

        response = s3_client.get_object(
            Bucket=WATERMARK_BUCKET,
            Key=WATERMARK_KEY
        )

        content = response["Body"].read().decode("utf-8")

        metadata = json.loads(content)

        watermark = metadata.get("last_updated")

        if watermark:

            logger.info(
                f"Gold watermark found: {watermark}"
            )

            return watermark

        logger.info(
            "Gold watermark is empty. "
            "Initial full load required."
        )

        return None

    except ClientError as e:

        error_code = e.response["Error"]["Code"]

        if error_code in ["NoSuchKey", "404"]:

            logger.info(
                "Gold watermark does not exist. "
                "Running initial full load."
            )

            return None

        logger.exception(
            "Failed to read Gold watermark."
        )

        raise

    except Exception:

        logger.exception(
            "Unexpected error while reading Gold watermark."
        )

        raise


# ==========================================================
# SAVE GOLD WATERMARK
# ==========================================================

def save_gold_watermark(watermark):

    try:

        metadata = {
            "last_updated": str(watermark),
            "updated_at": datetime.utcnow().isoformat()
        }

        s3_client.put_object(
            Bucket=WATERMARK_BUCKET,
            Key=WATERMARK_KEY,
            Body=json.dumps(metadata).encode("utf-8")
        )

        logger.info(
            f"Gold watermark updated successfully: {watermark}"
        )

    except Exception:

        logger.exception(
            "Failed to save Gold watermark."
        )

        raise


# ==========================================================
# DETERMINE LOAD MODE
# ==========================================================

gold_watermark = get_gold_watermark()

if gold_watermark is None:
    LOAD_MODE = "FULL"

else:
    LOAD_MODE = "INCREMENTAL"


logger.info(
    f"Gold Load Mode : {LOAD_MODE}"
)

logger.info(
    f"Gold Watermark : {gold_watermark}"
)

# ==========================================================
# READ FULL SILVER TABLE
# ==========================================================

def read_full_table(database, table_name):

    try:

        logger.info(
            f"FULL LOAD - Reading Silver table: {table_name}"
        )

        # Read through AWS Glue Data Catalog
        df = spark.table(
            f"{database}.{table_name}"
        )

        logger.info(
            f"{table_name}: Spark DataFrame created from Glue Data Catalog"
        )

        logger.info(
            f"{table_name}: Schema:"
        )

        df.printSchema()

        logger.info(
            f"{table_name}: Full row count = {df.count()}"
        )

        return df

    except Exception:

        logger.exception(
            f"Failed to read full Silver table: {table_name}"
        )

        raise
# ==========================================================
# READ SILVER CHANGE MANIFEST
# ==========================================================

def read_silver_change_manifest(watermark):

    try:

        logger.info(
            f"Reading Silver change manifest after watermark: {watermark}"
        )

        manifest_df = (
            spark.read
            .option("mergeSchema", "true")
            .parquet(SILVER_CHANGE_LOG_PATH)
        )

        logger.info(
            f"Silver change manifest columns: {manifest_df.columns}"
        )

        watermark_ts = F.to_timestamp(
            F.lit(watermark)
        )

        changed_df = (
            manifest_df
            .filter(
                F.col("changed_at") > watermark_ts
            )
        )

        logger.info(
            f"Silver change manifest rows after watermark = "
            f"{changed_df.count()}"
        )

        logger.info(
            "Silver change manifest read successfully."
        )

        return changed_df

    except Exception:

        logger.exception(
            "Failed to read Silver change manifest."
        )

        raise

# ==========================================================
# SPLIT SILVER CHANGE MANIFEST BY TABLE
# ==========================================================

# ==========================================================
# SPLIT SILVER CHANGE MANIFEST BY TABLE
# ==========================================================

TABLE_KEY_COLUMNS = {
    "orders": ["order_id"],
    "order_items": ["order_id", "order_item_id"],
    "payments": ["order_id"],
    "reviews": ["order_id"],
    "shipment": ["order_id"],
    "customers": ["customer_id"],
    "products": ["product_id"],
    "sellers": ["seller_id"],
    "inventory": ["inventory_id"]
}


def get_table_changes(manifest_df, table_name):

    try:

        if table_name not in TABLE_KEY_COLUMNS:
            raise ValueError(
                f"Unknown table_name in Gold ETL: {table_name}"
            )

        key_columns = TABLE_KEY_COLUMNS[table_name]

        # ------------------------------------------------------
        # Filter manifest for this source table
        # ------------------------------------------------------

        table_changes = (
            manifest_df
            .filter(
                F.col("table_name") == table_name
            )
        )

        # ------------------------------------------------------
        # IMPORTANT:
        # The manifest is a shared Parquet dataset.
        #
        # Different source tables have different primary-key
        # columns. If this run has no changes for a particular
        # table, those columns may not exist in the manifest
        # schema at all.
        #
        # Add missing key columns as NULL so downstream
        # .select("order_id"), etc. always works.
        # ------------------------------------------------------

        for key_column in key_columns:

            if key_column not in table_changes.columns:

                table_changes = table_changes.withColumn(
                    key_column,
                    F.lit(None).cast("string")
                )

        logger.info(
            f"{table_name}: Change rows = "
            f"{table_changes.count()}"
        )

        logger.info(
            f"{table_name}: Change columns = "
            f"{table_changes.columns}"
        )

        return table_changes

    except Exception:

        logger.exception(
            f"Failed to get change manifest for {table_name}"
        )

        raise

# ==========================================================
# READ CURRENT SILVER RECORDS FOR AFFECTED KEYS
# ==========================================================

# ==========================================================
# READ CURRENT SILVER RECORDS FOR AFFECTED KEYS
# USING HASH-BUCKET PARTITION PRUNING
# ==========================================================

def read_current_records_for_keys(
    database,
    table_name,
    key_df,
    key_columns
):

    try:

        if isinstance(key_columns, str):
            key_columns = [key_columns]

        logger.info(
            f"Reading current Silver records for affected keys: "
            f"{table_name}"
        )

        # --------------------------------------------------
        # Calculate Silver bucket using the SAME logic
        # used by Bronze -> Silver.
        # --------------------------------------------------

        bucket_expression = (
            F.pmod(
                F.abs(
                    F.hash(
                        *[
                            F.col(key).cast("string")
                            for key in key_columns
                        ]
                    )
                ),
                F.lit(32)
            )
        )

        key_df = (
            key_df
            .select(*key_columns)
            .dropDuplicates()
            .withColumn(
                "_bucket",
                bucket_expression
            )
        )

        # --------------------------------------------------
        # Determine required buckets
        # --------------------------------------------------

        buckets = [
            row["_bucket"]
            for row in (
                key_df
                .select("_bucket")
                .distinct()
                .collect()
            )
        ]

        if not buckets:
            logger.info(
                f"No affected keys for {table_name}."
            )

            return (
                spark.table(
                    f"{database}.{table_name}"
                )
                .limit(0)
            )

        logger.info(
            f"{table_name}: Reading Silver buckets "
            f"{buckets}"
        )

        # --------------------------------------------------
        # Read only required Silver partitions
        # --------------------------------------------------

        df = (
            spark.table(
                f"{database}.{table_name}"
            )
            .filter(
                F.col("_bucket").isin(buckets)
            )
        )

        # --------------------------------------------------
        # Keep ONLY the requested business keys
        # --------------------------------------------------

        current_df = (
            df.alias("source")
            .join(
                key_df.select(*key_columns).alias("keys"),
                key_columns,
                "inner"
            )
            .select("source.*")
        )

        logger.info(
            f"{table_name}: Current affected rows = "
            f"{current_df.count()}"
        )

        return current_df

    except Exception:

        logger.exception(
            f"Failed to read current records for "
            f"{table_name}"
        )

        raise
# ==========================================================
# READ SILVER DATA
# ==========================================================
# ==========================================================
# INITIALIZE INCREMENTAL DATAFRAMES
# ==========================================================

changed_orders_df = None
changed_order_items_df = None
changed_payments_df = None
changed_reviews_df = None
changed_shipment_df = None
changed_customers_df = None
changed_products_df = None
changed_sellers_df = None
changed_inventory_df = None

changed_orders = None
changed_customers = None
changed_products = None
changed_sellers = None
changed_inventory = None

if LOAD_MODE == "FULL":

    # ======================================================
    # FULL LOAD
    # ======================================================

    logger.info(
        "Starting INITIAL FULL LOAD from Silver."
    )

    orders_df = read_full_table(
        silver_db,
        "orders"
    )

    order_items_df = read_full_table(
        silver_db,
        "order_items"
    )

    payments_df = read_full_table(
        silver_db,
        "payments"
    )

    reviews_df = read_full_table(
        silver_db,
        "reviews"
    )

    shipment_df = read_full_table(
        silver_db,
        "shipment"
    )

    customers_df = read_full_table(
        silver_db,
        "customers"
    )

    products_df = read_full_table(
        silver_db,
        "products"
    )

    sellers_df = read_full_table(
        silver_db,
        "sellers"
    )

    inventory_df = read_full_table(
        silver_db,
        "inventory"
    )

else:
    logger.info(
    "Starting INCREMENTAL LOAD from Silver."
    )

    # ------------------------------------------------------
    # Read Silver Change Manifest
    # ------------------------------------------------------

    silver_changes_df = read_silver_change_manifest(
        gold_watermark
    )

    logger.info(
        f"FINAL manifest columns = {silver_changes_df.columns}"
    )

    # ------------------------------------------------------
    # No new Silver changes
    # ------------------------------------------------------

    if silver_changes_df.rdd.isEmpty():
        logger.info(
            "No new Silver changes detected."
        )

        logger.info(
            "Gold ETL completed successfully. "
            "No processing required."
        )

        job.commit()
        sys.exit(0)

    # ------------------------------------------------------
    # Split manifest by source table
    # ------------------------------------------------------

    changed_orders_df = get_table_changes(
        silver_changes_df,
        "orders"
    )

    changed_order_items_df = get_table_changes(
        silver_changes_df,
        "order_items"
    )

    changed_payments_df = get_table_changes(
        silver_changes_df,
        "payments"
    )

    changed_reviews_df = get_table_changes(
        silver_changes_df,
        "reviews"
    )

    changed_shipment_df = get_table_changes(
        silver_changes_df,
        "shipment"
    )

    changed_customers_df = get_table_changes(
        silver_changes_df,
        "customers"
    )

    changed_products_df = get_table_changes(
        silver_changes_df,
        "products"
    )

    changed_sellers_df = get_table_changes(
        silver_changes_df,
        "sellers"
    )

    changed_inventory_df = get_table_changes(
        silver_changes_df,
        "inventory"
    )

    logger.info(
        "Silver change manifest successfully split by table."
    )

    # ==========================================================
    # IDENTIFY SOFT DELETES FROM SILVER CHANGE MANIFEST
    # ==========================================================

    deleted_orders_df = (
        changed_orders_df
        .filter(F.col("operation") == "DELETE")
        .select("order_id")
        .distinct()
    )

    deleted_customers_df = (
        changed_customers_df
        .filter(F.col("operation") == "DELETE")
        .select("customer_id")
        .distinct()
    )

    deleted_products_df = (
        changed_products_df
        .filter(F.col("operation") == "DELETE")
        .select("product_id")
        .distinct()
    )

    deleted_sellers_df = (
        changed_sellers_df
        .filter(F.col("operation") == "DELETE")
        .select("seller_id")
        .distinct()
    )

    deleted_inventory_df = (
        changed_inventory_df
        .filter(F.col("operation") == "DELETE")
        .select("inventory_id")
        .distinct()
    )

    logger.info(
        f"Deleted orders = {deleted_orders_df.count()}"
    )

    logger.info(
        f"Deleted customers = {deleted_customers_df.count()}"
    )

    logger.info(
        f"Deleted products = {deleted_products_df.count()}"
    )

    logger.info(
        f"Deleted sellers = {deleted_sellers_df.count()}"
    )

    logger.info(
        f"Deleted inventory = {deleted_inventory_df.count()}"
    )

    # ------------------------------------------------------
    # 2. Identify affected business keys
    # ------------------------------------------------------

    changed_orders = (

        changed_orders_df
        .select("order_id")

        .union(
            changed_order_items_df
            .select("order_id")
        )

        .union(
            changed_payments_df
            .select("order_id")
        )

        .union(
            changed_reviews_df
            .select("order_id")
        )

        .union(
            changed_shipment_df
            .select("order_id")
        )

        .distinct()
        .cache()

    )

    changed_customers = (
        changed_customers_df
        .select("customer_id")
        .distinct()
        .cache()
    )

    changed_products = (
        changed_products_df
        .select("product_id")
        .distinct()
        .cache()
    )

    changed_sellers = (
        changed_sellers_df
        .select("seller_id")
        .distinct()
        .cache()
    )

    changed_inventory = (
        changed_inventory_df
        .select("inventory_id")
        .distinct()
        .cache()
    )

    logger.info(
        f"Affected orders = {changed_orders.count()}"
    )

    logger.info(
        f"Affected customers = {changed_customers.count()}"
    )

    logger.info(
        f"Affected products = {changed_products.count()}"
    )

    logger.info(
        f"Affected sellers = {changed_sellers.count()}"
    )

    logger.info(
        f"Affected inventory = {changed_inventory.count()}"
    )

    # ------------------------------------------------------
    # 3. Read CURRENT Silver state for affected keys
    # ------------------------------------------------------

    orders_df = read_current_records_for_keys(
        silver_db,
        "orders",
        changed_orders,
        ["order_id"]
    )

    order_items_df = read_current_records_for_keys(
        silver_db,
        "order_items",
        changed_orders,
        ["order_id"]
    )

    payments_df = read_current_records_for_keys(
        silver_db,
        "payments",
        changed_orders,
        ["order_id"]
    )

    reviews_df = read_current_records_for_keys(
        silver_db,
        "reviews",
        changed_orders,
        ["order_id"]
    )

    shipment_df = read_current_records_for_keys(
        silver_db,
        "shipment",
        changed_orders,
        ["order_id"]
    )

    customers_df = read_current_records_for_keys(
        silver_db,
        "customers",
        changed_customers,
        ["customer_id"]
    )

    products_df = read_current_records_for_keys(
        silver_db,
        "products",
        changed_products,
        ["product_id"]
    )

    sellers_df = read_current_records_for_keys(
        silver_db,
        "sellers",
        changed_sellers,
        ["seller_id"]
    )

    inventory_df = read_current_records_for_keys(
        silver_db,
        "inventory",
        changed_inventory,
        ["inventory_id"]
    )



# ----------------------------------
# Read Glue Catalog Table
# ----------------------------------

def read_table(database, table_name):

    logger.info(f"Reading {table_name}")

    dyf = glueContext.create_dynamic_frame.from_catalog(
        database=database,
        table_name=table_name,
        transformation_ctx=f"gold_{table_name}"
    )

    return dyf.toDF()

def delete_s3_prefix(s3_path):

    parsed = urlparse(s3_path)

    bucket = parsed.netloc

    prefix = parsed.path.lstrip("/")

    s3 = boto3.client("s3")

    paginator = s3.get_paginator("list_objects_v2")

    pages = paginator.paginate(
        Bucket=bucket,
        Prefix=prefix
    )

    deleted_count = 0

    for page in pages:

        if "Contents" not in page:
            continue

        objects = [
            {"Key": obj["Key"]}
            for obj in page["Contents"]
        ]

        s3.delete_objects(
            Bucket=bucket,
            Delete={
                "Objects": objects
            }
        )

        deleted_count += len(objects)

    logger.info(
        f"Deleted {deleted_count} existing objects from {s3_path}"
    )

orders_df = orders_df.select(
    "order_id",
    "customer_id",
    "order_status",
    "order_purchase_timestamp",
    "order_approved_at",
    "order_delivered_customer_date",
    "order_estimated_delivery_date",
    "created_at",
    "updated_at",
    "is_deleted"
).alias("o")

order_items_df = order_items_df.select(
    "order_id",
    "order_item_id",
    "product_id",
    "seller_id",
    "shipping_limit_date",
    "price",
    "freight_value",
    "created_at",
    "is_deleted"
).alias("oi")

payments_df = payments_df.select(
    "order_id",
    "payment_type",
    "payment_installments",
    "payment_value",
    "created_at",
    "updated_at",
    "is_deleted"
).alias("p")

reviews_df = reviews_df.select(
    "order_id",
    "review_score",
    "review_creation_date",
    "review_answer_timestamp",
    "created_at",
    "updated_at",
    "is_deleted"
)

review_window = Window.partitionBy(
    "order_id"
).orderBy(
    F.col("review_answer_timestamp").desc_nulls_last()
)

reviews_df = (
    reviews_df

    .withColumn(
        "row_num",
        F.row_number().over(review_window)
    )

    .filter(
        F.col("row_num") == 1
    )

    .drop("row_num")

    .alias("r")
)

duplicate_review_count = (
    reviews_df
    .groupBy("order_id")
    .count()
    .filter(F.col("count") > 1)
    .count()
)

if duplicate_review_count > 0:

    logger.error(
        f"Review Deduplication Failed. Duplicate order_id count : {duplicate_review_count}"
    )

    raise Exception(
        "Duplicate order_id still exists in reviews table."
    )

logger.info(
    "Review Deduplication validation passed."
)

shipment_df = shipment_df.select(
    "order_id",
    "order_item_id",
    "carrier_name",
    "shipping_cost",
    "shipment_status",
    "shipped_timestamp",
    "delivered_timestamp"
).alias("s")

customers_df = customers_df.select(
    "customer_id",
    "customer_unique_id",
    "customer_zip_code_prefix",
    "customer_city",
    "customer_state",
    "loyalty_points",
    "created_at",
    "updated_at",
    "is_deleted"
).alias("c")

products_df = products_df.select(
    "product_id",
    "product_category_name",
    "product_name_lenght",
    "product_description_lenght",
    "product_photos_qty",
    "product_weight_g",
    "product_length_cm",
    "product_height_cm",
    "product_width_cm",
    "created_at",
    "updated_at",
    "is_deleted"
).alias("pr")

sellers_df = sellers_df.select(
    "seller_id",
    "seller_zip_code_prefix",
    "seller_city",
    "seller_state",
    "created_at",
    "updated_at",
    "is_deleted"
).alias("se")

inventory_df = inventory_df.select(
    "inventory_id",
    "warehouse_id",
    "warehouse_city",
    "warehouse_state",
    "product_id",
    "available_stock",
    "reserved_stock",
    "safety_stock",
    "reorder_level",
    "inventory_status",
    "last_updated"
).alias("i")

# ----------------------------------
# Fact Sales
# Grain:
# One row = One Order Item Sold
# ----------------------------------

fact_sales_df = (
    order_items_df.join(
        orders_df,
        F.col("oi.order_id") == F.col("o.order_id"),
        "inner"
    )
)

fact_sales_df = (
    fact_sales_df.join(
        shipment_df,
        (
            (F.col("oi.order_id") == F.col("s.order_id")) &
            (F.col("oi.order_item_id") == F.col("s.order_item_id"))
        ),
        "left"
    )
)

fact_sales_df = (
    fact_sales_df.join(
        reviews_df,
        F.col("oi.order_id") == F.col("r.order_id"),
        "left"
    )
)

# ----------------------------------
# Create Business KPI Columns
# ----------------------------------

fact_sales_df = (
    fact_sales_df

    # Total Order Value
    .withColumn(
        "total_order_value",
        F.col("price") + F.col("freight_value")
    )

    # Purchase Date
    .withColumn(
        "purchase_date",
        F.to_date("order_purchase_timestamp")
    )

    # Purchase Year
    .withColumn(
        "purchase_year",
        F.year("order_purchase_timestamp")
    )

    # Purchase Month
    .withColumn(
        "purchase_month",
        F.month("order_purchase_timestamp")
    )

    # Delivery Days
    .withColumn(
        "delivery_days",
        F.datediff(
            F.to_date("order_delivered_customer_date"),
            F.to_date("order_purchase_timestamp")
        )
    )

    # Delivery Delay Days
    .withColumn(
        "delivery_delay_days",
        F.datediff(
            F.to_date("order_delivered_customer_date"),
            F.to_date("order_estimated_delivery_date")
        )
    )

    # Late Delivery Flag
    .withColumn(
        "late_delivery_flag",
        F.when(
            F.col("order_delivered_customer_date").isNull(),
            "Pending"
        )
        .when(
            F.col("delivery_delay_days") > 0,
            "Yes"
        )
        .otherwise("No")
    )

    # Review Category
    .withColumn(
        "review_category",
        F.when(
            F.col("review_score").isNull(),
            "No Review"
        )
        .when(
            F.col("review_score") >= 4,
            "Good"
        )
        .when(
            F.col("review_score") == 3,
            "Average"
        )
        .otherwise("Poor")
    )
)

# ----------------------------------
# Final Fact Sales Schema
# ----------------------------------

fact_sales_df = fact_sales_df.select(

    # Keys
    F.col("oi.order_id").alias("order_id"),
    F.col("oi.order_item_id").alias("order_item_id"),
    F.col("o.customer_id").alias("customer_id"),
    F.col("oi.product_id").alias("product_id"),
    F.col("oi.seller_id").alias("seller_id"),

    # Order Information
    F.col("o.order_status"),
    F.col("o.order_purchase_timestamp"),
    F.col("o.order_approved_at"),
    F.col("o.order_delivered_customer_date"),
    F.col("o.order_estimated_delivery_date"),

    # Product Information
    F.col("oi.shipping_limit_date"),
    F.col("oi.price"),
    F.col("oi.freight_value"),

    # Shipment Information
    F.col("s.carrier_name"),
    F.col("s.shipping_cost"),
    F.col("s.shipment_status"),
    F.col("s.shipped_timestamp"),
    F.col("s.delivered_timestamp"),

    # Reviews
    F.col("r.review_score"),
    F.col("r.review_creation_date"),
    F.col("r.review_answer_timestamp"),

    # Derived Business KPIs
    F.col("total_order_value"),
    F.col("purchase_date"),
    F.col("purchase_year"),
    F.col("purchase_month"),
    F.col("delivery_days"),
    F.col("delivery_delay_days"),
    F.col("late_delivery_flag"),
    F.col("review_category")
)

fact_sales_df = (
    fact_sales_df
    .withColumn(
        "_bucket",
        F.pmod(
            F.abs(
                F.hash(
                    F.col("order_id").cast("string"),
                    F.col("order_item_id").cast("string")
                )
            ),
            F.lit(32)
        )
    )
)

fact_sales_df = (
    fact_sales_df

    .withColumn(
        "order_purchase_timestamp",
        F.col("order_purchase_timestamp").cast("timestamp")
    )

    .withColumn(
        "order_approved_at",
        F.col("order_approved_at").cast("timestamp")
    )

    .withColumn(
        "order_delivered_customer_date",
        F.col("order_delivered_customer_date").cast("timestamp")
    )

    .withColumn(
        "order_estimated_delivery_date",
        F.col("order_estimated_delivery_date").cast("timestamp")
    )

    .withColumn(
        "shipping_limit_date",
        F.col("shipping_limit_date").cast("timestamp")
    )

    .withColumn(
        "review_creation_date",
        F.col("review_creation_date").cast("timestamp")
    )

    .withColumn(
        "review_answer_timestamp",
        F.col("review_answer_timestamp").cast("timestamp")
    )
)

fact_sales_df = fact_sales_df.cache()

# ----------------------------------
# Aggregate Payments
# One row per Order
# ----------------------------------

payments_agg_df = (

    payments_df

    .groupBy("order_id")

    .agg(

        F.concat_ws(
            ", ",
            F.collect_set("payment_type")
        ).alias("payment_type"),

        F.sum("payment_installments")
            .alias("payment_installments"),

        F.sum("payment_value")
            .alias("payment_value")

    )

    .alias("p")

)

# ----------------------------------
# Build Fact Payments
# ----------------------------------

fact_payments_df = (

    payments_agg_df.join(

        orders_df,

        F.col("p.order_id") == F.col("o.order_id"),

        "inner"

    )

)

# ----------------------------------
# Payment Business Columns
# ----------------------------------

fact_payments_df = (

    fact_payments_df

    .withColumn(
        "purchase_date",
        F.to_date("order_purchase_timestamp")
    )

    .withColumn(
        "purchase_year",
        F.year("order_purchase_timestamp")
    )

    .withColumn(
        "purchase_month",
        F.month("order_purchase_timestamp")
    )
)

# ----------------------------------
# Final Fact Payments Schema
# ----------------------------------

fact_payments_df = fact_payments_df.select(

    # Keys
    F.col("p.order_id").alias("order_id"),
    F.col("o.customer_id").alias("customer_id"),

    # Payment Information
    F.col("p.payment_type"),
    F.col("p.payment_installments"),
    F.col("p.payment_value"),

    # Order Information
    F.col("o.order_status"),
    F.col("o.order_purchase_timestamp"),

    # Date Columns
    F.col("purchase_date"),
    F.col("purchase_year"),
    F.col("purchase_month")
)

fact_payments_df = (
    fact_payments_df
    .withColumn(
        "_bucket",
        F.pmod(
            F.abs(
                F.hash(
                    F.col("order_id").cast("string")
                )
            ),
            F.lit(32)
        )
    )
)

fact_payments_df = (
    fact_payments_df
    .withColumn(
        "order_purchase_timestamp",
        F.col("order_purchase_timestamp").cast("timestamp")
    )
)

fact_payments_df = fact_payments_df.cache()

# ----------------------------------
# Validate Fact Tables
# ----------------------------------

logger.info(f"Fact Sales Count : {fact_sales_df.count()}")

logger.info(f"Fact Payments Count : {fact_payments_df.count()}")

# ----------------------------------
# Monitor Pending Deliveries
# ----------------------------------

pending_delivery_count = (

    fact_sales_df

    .filter(
        F.col("late_delivery_flag") == "Pending"
    )

    .count()

)

logger.info(
    f"Pending Deliveries : {pending_delivery_count}"
)

# ----------------------------------
# Monitor Orders Without Reviews
# ----------------------------------

no_review_count = (

    fact_sales_df

    .filter(
        F.col("review_category") == "No Review"
    )

    .count()

)

logger.info(
    f"Orders Without Reviews : {no_review_count}"
)

# ----------------------------------
# Validate Customer Dimension
# ----------------------------------

customer_duplicate_count = (
    customers_df
        .groupBy("customer_id")
        .count()
        .filter(F.col("count") > 1)
        .count()
)

if customer_duplicate_count > 0:

    logger.error(
        f"Customer Dimension validation failed. Duplicate customer_id count : {customer_duplicate_count}"
    )

    raise Exception(
        "Duplicate customer_id found in Silver customers table."
    )

logger.info("Customer Dimension validation passed.")

# ----------------------------------
# Build Customer Dimension
# ----------------------------------

dim_customer_df = customers_df

# ----------------------------------
# Final Customer Dimension Schema
# ----------------------------------

dim_customer_df = dim_customer_df.select(

    "customer_id",
    "customer_unique_id",
    "customer_zip_code_prefix",
    "customer_city",
    "customer_state",
    "loyalty_points"

)
dim_customer_df = (
    dim_customer_df
    .withColumn(
        "_bucket",
        F.pmod(
            F.abs(
                F.hash(
                    F.col("customer_id").cast("string")
                )
            ),
            F.lit(32)
        )
    )
)

# ----------------------------------
# Validate Product Dimension
# ----------------------------------

product_duplicate_count = (
    products_df
        .groupBy("product_id")
        .count()
        .filter(F.col("count") > 1)
        .count()
)

if product_duplicate_count > 0:

    logger.error(
        f"Product Dimension validation failed. Duplicate product_id count : {product_duplicate_count}"
    )

    raise Exception(
        "Duplicate product_id found in Silver products table."
    )

logger.info("Product Dimension validation passed.")

# ----------------------------------
# Build Product Dimension
# ----------------------------------

dim_product_df = products_df

# ----------------------------------
# Final Product Dimension Schema
# ----------------------------------

dim_product_df = dim_product_df.select(

    "product_id",
    "product_category_name",
    "product_name_lenght",
    "product_description_lenght",
    "product_photos_qty",
    "product_weight_g",
    "product_length_cm",
    "product_height_cm",
    "product_width_cm"

)

dim_product_df = (
    dim_product_df
    .withColumn(
        "_bucket",
        F.pmod(
            F.abs(
                F.hash(
                    F.col("product_id").cast("string")
                )
            ),
            F.lit(32)
        )
    )
)

# ----------------------------------
# Validate Seller Dimension
# ----------------------------------

seller_duplicate_count = (
    sellers_df
        .groupBy("seller_id")
        .count()
        .filter(F.col("count") > 1)
        .count()
)

if seller_duplicate_count > 0:

    logger.error(
        f"Seller Dimension validation failed. Duplicate seller_id count : {seller_duplicate_count}"
    )

    raise Exception(
        "Duplicate seller_id found in Silver sellers table."
    )

logger.info("Seller Dimension validation passed.")

# ----------------------------------
# Build Seller Dimension
# ----------------------------------

dim_seller_df = sellers_df

# ----------------------------------
# Final Seller Dimension Schema
# ----------------------------------

dim_seller_df = dim_seller_df.select(

    "seller_id",
    "seller_zip_code_prefix",
    "seller_city",
    "seller_state"

)

dim_seller_df = (
    dim_seller_df
    .withColumn(
        "_bucket",
        F.pmod(
            F.abs(
                F.hash(
                    F.col("seller_id").cast("string")
                )
            ),
            F.lit(32)
        )
    )
)

# ----------------------------------
# Validate Inventory Dimension
# ----------------------------------

inventory_duplicate_count = (
    inventory_df
        .groupBy("inventory_id")
        .count()
        .filter(F.col("count") > 1)
        .count()
)

if inventory_duplicate_count > 0:

    logger.error(
        f"Inventory Dimension validation failed. Duplicate inventory_id count : {inventory_duplicate_count}"
    )

    raise Exception(
        "Duplicate inventory_id found in Silver inventory table."
    )

logger.info("Inventory Dimension validation passed.")

# ----------------------------------
# Build Inventory Dimension
# ----------------------------------

dim_inventory_df = inventory_df

# ----------------------------------
# Final Inventory Dimension Schema
# ----------------------------------

dim_inventory_df = dim_inventory_df.select(

    "inventory_id",
    "warehouse_id",
    "warehouse_city",
    "warehouse_state",
    "product_id",
    "available_stock",
    "reserved_stock",
    "safety_stock",
    "reorder_level",
    "inventory_status",
    "last_updated"

)

dim_inventory_df = (
    dim_inventory_df
    .withColumn(
        "_bucket",
        F.pmod(
            F.abs(
                F.hash(
                    F.col("inventory_id").cast("string")
                )
            ),
            F.lit(32)
        )
    )
)

# ----------------------------------
# Sales Daily Mart
# Grain:
# One row = One Purchase Date
# ----------------------------------

fact_sales_daily_df = (

    fact_sales_df

    .groupBy(

        "purchase_date",
        "purchase_year",
        "purchase_month"

    )

    .agg(

        F.countDistinct("order_id").alias("total_orders"),

        F.count("order_item_id").alias("total_items_sold"),

        F.sum("total_order_value").alias("total_sales"),

        F.avg("total_order_value").alias("average_order_value"),

        F.sum("freight_value").alias("total_freight"),

        F.avg("delivery_days").alias("average_delivery_days"),

        F.sum(
            F.when(
                F.col("late_delivery_flag") == "Yes",
                1
            ).otherwise(0)
        ).alias("late_deliveries")

    )

)

# ----------------------------------
# Validate Inventory Status Consistency
# ----------------------------------

inventory_status_validation = (

    dim_inventory_df

    .groupBy(
        "warehouse_id",
        "product_id"
    )

    .agg(
        F.countDistinct("inventory_status").alias("status_count")
    )

    .filter(
        F.col("status_count") > 1
    )

    .count()

)

if inventory_status_validation > 0:

    logger.error(
        f"Inventory Summary validation failed. Multiple inventory_status values found for the same warehouse_id and product_id. Count : {inventory_status_validation}"
    )

    raise Exception(
        "Inventory Summary validation failed due to inconsistent inventory_status."
    )

logger.info("Inventory Status validation passed.")

# ----------------------------------
# Inventory Summary Mart
# Grain:
# One row = One Product in One Warehouse
# ----------------------------------

inventory_summary_df = (

    dim_inventory_df

    .groupBy(
        "warehouse_id",
        "warehouse_city",
        "warehouse_state",
        "product_id"
    )

    .agg(

        F.sum("available_stock").alias(
            "total_available_stock"
        ),

        F.sum("reserved_stock").alias(
            "total_reserved_stock"
        ),

        F.sum("safety_stock").alias(
            "total_safety_stock"
        ),

        F.max("reorder_level").alias(
            "reorder_level"
        ),

        F.first("inventory_status").alias(
            "inventory_status"
        ),

        F.max("last_updated").alias(
            "last_updated"
        )

    )

    # --------------------------------------------------
    # Gold partition bucket
    # Same deterministic hashing logic used elsewhere
    # --------------------------------------------------

    .withColumn(
        "_bucket",
        F.pmod(
            F.abs(
                F.hash(
                    F.col("warehouse_id").cast("string"),
                    F.col("product_id").cast("string")
                )
            ),
            F.lit(32)
        )
    )
)

logger.info(
    f"Inventory Summary row count = {inventory_summary_df.count()}"
)

# ==========================================================
# READ EXISTING GOLD FACT SALES
# ONLY REQUIRED BUCKETS
# ==========================================================

def read_existing_gold_fact_sales_for_dates(
    changed_dates,
    changed_fact_sales
):
    target_path = GOLD_TABLE_PATHS["fact_sales"]

    try:

        # --------------------------------------------------
        # Calculate affected Gold buckets
        # fact_sales is partitioned by _bucket
        # --------------------------------------------------

        affected_buckets = (
            changed_fact_sales
            .select("_bucket")
            .distinct()
        )

        bucket_rows = (
            affected_buckets
            .collect()
        )

        buckets = [
            int(row["_bucket"])
            for row in bucket_rows
        ]

        if not buckets:

            logger.info(
                "No affected Gold fact_sales buckets."
            )

            return None

        logger.info(
            f"Gold fact_sales affected buckets: {buckets}"
        )

        # --------------------------------------------------
        # Read ONLY affected Gold partition directories
        # --------------------------------------------------

        partition_paths = [
            f"{target_path}_bucket={bucket}"
            for bucket in buckets
        ]

        existing_fact_sales = (
            spark.read
            .parquet(*partition_paths)
        )

        # --------------------------------------------------
        # Keep only affected purchase dates
        # --------------------------------------------------

        existing_affected = (
            existing_fact_sales
            .join(
                changed_dates,
                "purchase_date",
                "inner"
            )
        )

        # --------------------------------------------------
        # Remove old versions of changed fact-sales keys
        # --------------------------------------------------

        changed_keys = (
            changed_fact_sales
            .select(
                "order_id",
                "order_item_id"
            )
            .distinct()
        )

        existing_affected = (
            existing_affected
            .join(
                changed_keys,
                [
                    "order_id",
                    "order_item_id"
                ],
                "left_anti"
            )
        )

        return existing_affected

    except Exception:

        logger.exception(
            "Failed to read affected Gold fact_sales."
        )

        raise

# ==========================================================
# READ EXISTING GOLD FACT SALES FOR CHANGED KEYS
# Used to identify affected purchase dates when records
# are deleted from Silver.
# ==========================================================

def read_existing_gold_fact_sales_for_keys(
    changed_fact_sales
):

    target_path = GOLD_TABLE_PATHS["fact_sales"]

    try:

        # --------------------------------------------------
        # Determine affected Gold buckets
        # --------------------------------------------------

        affected_buckets = (
            changed_fact_sales
            .select("_bucket")
            .distinct()
        )

        bucket_rows = (
            affected_buckets
            .collect()
        )

        buckets = [
            int(row["_bucket"])
            for row in bucket_rows
        ]

        if not buckets:

            logger.info(
                "No affected Gold fact_sales buckets."
            )

            return None

        logger.info(
            f"Reading existing Gold fact_sales "
            f"for buckets: {buckets}"
        )

        # --------------------------------------------------
        # Read only affected Gold partitions
        # --------------------------------------------------

        partition_paths = [
            f"{target_path}_bucket={bucket}"
            for bucket in buckets
        ]

        existing_fact_sales = (
            spark.read
            .parquet(*partition_paths)
        )

        # --------------------------------------------------
        # Match exact changed business keys
        # --------------------------------------------------

        changed_keys = (
            changed_fact_sales
            .select(
                "order_id",
                "order_item_id"
            )
            .dropDuplicates()
        )

        existing_affected = (
            existing_fact_sales
            .join(
                changed_keys,
                [
                    "order_id",
                    "order_item_id"
                ],
                "inner"
            )
        )

        return existing_affected

    except Exception:

        logger.exception(
            "Failed to read existing Gold "
            "fact_sales for changed keys."
        )

        raise

# ==========================================================
# DELETED FACT SALES ROWS
# ==========================================================

deleted_fact_sales_rows_df = None
deleted_fact_sales_keys_df = None

if LOAD_MODE == "INCREMENTAL":

    # ------------------------------------------------------
    # Deleted order-item keys
    # ------------------------------------------------------

    deleted_order_item_keys_df = (
        changed_order_items_df
        .filter(
            F.col("operation") == "DELETE"
        )
        .select(
            "order_id",
            "order_item_id"
        )
        .distinct()
    )

    # ------------------------------------------------------
    # Deleted order keys
    # ------------------------------------------------------

    deleted_order_ids_df = (
        changed_orders_df
        .filter(
            F.col("operation") == "DELETE"
        )
        .select(
            "order_id"
        )
        .distinct()
    )

    # ------------------------------------------------------
    # Read existing Gold fact_sales
    # ------------------------------------------------------

    existing_gold_fact_sales = (
        spark.read.parquet(
            GOLD_TABLE_PATHS["fact_sales"]
        )
    )

    # ------------------------------------------------------
    # Identify old fact_sales rows affected by DELETE
    #
    # 1. Entire order deleted
    # 2. Individual order item deleted
    # ------------------------------------------------------

    deleted_fact_sales_rows_df = (
        existing_gold_fact_sales

        .join(
            deleted_order_ids_df
            .withColumn(
                "_delete_order",
                F.lit(1)
            ),
            "order_id",
            "left"
        )

        .join(
            deleted_order_item_keys_df
            .withColumn(
                "_delete_item",
                F.lit(1)
            ),
            [
                "order_id",
                "order_item_id"
            ],
            "left"
        )

        .filter(
            F.col("_delete_order").isNotNull()
            |
            F.col("_delete_item").isNotNull()
        )

        .drop(
            "_delete_order",
            "_delete_item"
        )
    )

    # ------------------------------------------------------
    # Keep only business keys for Gold DELETE
    # ------------------------------------------------------

    deleted_fact_sales_keys_df = (
        deleted_fact_sales_rows_df
        .select(
            "order_id",
            "order_item_id"
        )
        .distinct()
    )

# ==========================================================
# BUILD GOLD DATASETS
#
# IMPORTANT:
# This section is OUTSIDE the previous if/else.
# Therefore it executes for BOTH FULL and INCREMENTAL.
# ==========================================================

if LOAD_MODE == "FULL":

    logger.info(
        "Building complete Gold datasets."
    )

    gold_fact_sales = fact_sales_df

    gold_fact_payments = fact_payments_df

    gold_dim_customer = dim_customer_df

    gold_dim_product = dim_product_df

    gold_dim_seller = dim_seller_df

    gold_dim_inventory = dim_inventory_df

    gold_sales_daily = fact_sales_daily_df

    gold_inventory_summary = inventory_summary_df


else:

    logger.info(
        "Building incremental Gold datasets."
    )

    # ======================================================
    # FACT SALES
    # ======================================================

    gold_fact_sales = fact_sales_df

    # ======================================================
    # FACT PAYMENTS
    # ======================================================

    gold_fact_payments = fact_payments_df

    # ======================================================
    # CUSTOMER DIMENSION
    # ======================================================

    gold_dim_customer = dim_customer_df

    # ======================================================
    # PRODUCT DIMENSION
    # ======================================================
    gold_dim_product = dim_product_df

    # ======================================================
    # SELLER DIMENSION
    # ======================================================

    gold_dim_seller = dim_seller_df

    # ======================================================
    # INVENTORY DIMENSION
    # ======================================================

    gold_dim_inventory = dim_inventory_df

    # ======================================================
    # SALES DAILY
    # ======================================================

    # ------------------------------------------------------
    # Current affected dates
    # Handles INSERT / UPDATE
    # ------------------------------------------------------

    current_changed_dates = (
        fact_sales_df
        .select(
            "purchase_date",
            "purchase_year",
            "purchase_month"
        )
        .distinct()
    )

    # ------------------------------------------------------
    # Deleted fact_sales dates
    # Handles DELETE
    #
    # deleted_fact_sales_rows_df contains the OLD Gold rows
    # so we can still identify purchase_date after deletion.
    # ------------------------------------------------------

    if LOAD_MODE == "INCREMENTAL":

        deleted_fact_sales_dates = (
            deleted_fact_sales_rows_df
            .select(
                "purchase_date",
                "purchase_year",
                "purchase_month"
            )
            .distinct()
        )

    else:

        deleted_fact_sales_dates = (
            spark.createDataFrame(
                [],
                current_changed_dates.schema
            )
        )

    # ------------------------------------------------------
    # Combine current + deleted affected dates
    # ------------------------------------------------------

    changed_dates = (
        current_changed_dates
        .unionByName(
            deleted_fact_sales_dates
        )
        .distinct()
    )

    # ------------------------------------------------------
    # Rebuild only affected dates
    # ------------------------------------------------------

    if changed_dates.count() > 0:

        # --------------------------------------------------
        # Include BOTH:
        #
        # 1. Current changed fact_sales rows
        # 2. Old deleted fact_sales rows
        #
        # This allows us to calculate the affected Gold
        # buckets even when a record was deleted.
        # --------------------------------------------------

        fact_sales_for_lookup = (
            fact_sales_df
            .unionByName(
                deleted_fact_sales_rows_df,
                allowMissingColumns=True
            )
        )

        existing_fact_sales_for_dates = (
            read_existing_gold_fact_sales_for_dates(
                changed_dates,
                fact_sales_for_lookup
            )
        )

        if existing_fact_sales_for_dates is not None:

            sales_daily_source = (
                existing_fact_sales_for_dates
                .unionByName(
                    fact_sales_df,
                    allowMissingColumns=True
                )
            )

        else:

            sales_daily_source = fact_sales_df

        # --------------------------------------------------
        # Recalculate daily aggregate
        # --------------------------------------------------

        gold_sales_daily = (
            sales_daily_source

            .groupBy(
                "purchase_date",
                "purchase_year",
                "purchase_month"
            )

            .agg(

                F.countDistinct(
                    "order_id"
                ).alias(
                    "total_orders"
                ),

                F.count(
                    "order_item_id"
                ).alias(
                    "total_items_sold"
                ),

                F.sum(
                    "total_order_value"
                ).alias(
                    "total_sales"
                ),

                F.avg(
                    "total_order_value"
                ).alias(
                    "average_order_value"
                ),

                F.sum(
                    "freight_value"
                ).alias(
                    "total_freight"
                ),

                F.avg(
                    "delivery_days"
                ).alias(
                    "average_delivery_days"
                ),

                F.sum(
                    F.when(
                        F.col("late_delivery_flag") == "Yes",
                        1
                    ).otherwise(0)
                ).alias(
                    "late_deliveries"
                )
            )
        )

    else:

        gold_sales_daily = spark.createDataFrame(
            [],
            fact_sales_daily_df.schema
        )

# ======================================================
# INVENTORY SUMMARY
# ======================================================

if LOAD_MODE == "FULL":

    # --------------------------------------------------
    # FULL LOAD
    # Use the complete inventory summary already built
    # --------------------------------------------------

    gold_inventory_summary = inventory_summary_df

    logger.info(
        f"FULL LOAD - Inventory Summary row count = "
        f"{gold_inventory_summary.count()}"
    )

else:

    # --------------------------------------------------
    # INCREMENTAL LOAD
    # --------------------------------------------------

    # Read current inventory state
    current_inventory = read_full_table(
        silver_db,
        "inventory"
    )

    # --------------------------------------------------
    # Identify affected warehouse + product groups
    #
    # Current inventory handles INSERT / UPDATE.
    # Changed inventory manifest handles DELETE.
    # --------------------------------------------------

    current_inventory_groups = (
        current_inventory
        .join(
            changed_inventory,
            "inventory_id",
            "inner"
        )
        .select(
            "warehouse_id",
            "product_id"
        )
        .distinct()
    )

    deleted_inventory_groups = (
    spark.read
    .parquet(
        GOLD_TABLE_PATHS["dim_inventory"]
    )
    .join(
        changed_inventory_df
        .filter(
            F.col("operation") == "DELETE"
        )
        .select(
            "inventory_id"
        )
        .distinct(),
        "inventory_id",
        "inner"
    )
    .select(
        "warehouse_id",
        "product_id"
    )
    .distinct()
)

    affected_inventory_groups = (
        current_inventory_groups
        .unionByName(
            deleted_inventory_groups
        )
        .distinct()
    )

    # --------------------------------------------------
    # Rebuild summary using ALL CURRENT inventory
    # records belonging to affected groups
    # --------------------------------------------------

    inventory_for_summary = (
        current_inventory
        .join(
            affected_inventory_groups,
            [
                "warehouse_id",
                "product_id"
            ],
            "inner"
        )
    )

    gold_inventory_summary = (
        inventory_for_summary

        .groupBy(
            "warehouse_id",
            "warehouse_city",
            "warehouse_state",
            "product_id"
        )

        .agg(

            F.sum(
                "available_stock"
            ).alias(
                "total_available_stock"
            ),

            F.sum(
                "reserved_stock"
            ).alias(
                "total_reserved_stock"
            ),

            F.sum(
                "safety_stock"
            ).alias(
                "total_safety_stock"
            ),

            F.max(
                "reorder_level"
            ).alias(
                "reorder_level"
            ),

            F.first(
                "inventory_status"
            ).alias(
                "inventory_status"
            ),

            F.max(
                "last_updated"
            ).alias(
                "last_updated"
            )
        )

        # --------------------------------------------------
        # Gold partition bucket
        # --------------------------------------------------

        .withColumn(
            "_bucket",
            F.pmod(
                F.abs(
                    F.hash(
                        F.col("warehouse_id").cast("string"),
                        F.col("product_id").cast("string")
                    )
                ),
                F.lit(32)
            )
        )
    )

    logger.info(
        f"INCREMENTAL LOAD - Inventory Summary row count = "
        f"{gold_inventory_summary.count()}"
    )


# ======================================================
# FINAL INVENTORY SUMMARY VALIDATION
# ======================================================

logger.info(
    "========== INVENTORY SUMMARY BEFORE WRITE =========="
)

logger.info(
    f"inventory_summary columns = "
    f"{gold_inventory_summary.columns}"
)

logger.info(
    f"inventory_summary schema = "
    f"{gold_inventory_summary.schema}"
)

inventory_summary_count = gold_inventory_summary.count()

logger.info(
    f"inventory_summary row count before write = "
    f"{inventory_summary_count}"
)

gold_inventory_summary.show(
    10,
    truncate=False
)

if LOAD_MODE == "FULL" and inventory_summary_count == 0:
    raise Exception(
        "FULL LOAD ERROR: inventory_summary is empty. "
        "Expected inventory summary records."
    )

# ==========================================================
# WRITE DATA TO S3 + UPDATE GLUE DATA CATALOG
# ==========================================================

def write_to_gold_catalog(
    df,
    table_name,
    target_path,
    partition_columns,
    affected_partitions_df=None
):

    logger.info(
        f"Writing {table_name} to S3 and updating Glue Data Catalog."
    )

    # ======================================================
    # INCREMENTAL LOAD
    # Delete ONLY affected partition folders first
    # ======================================================

    if (
            LOAD_MODE == "INCREMENTAL"
            and affected_partitions_df is not None
    ):

        affected_rows = (
            affected_partitions_df
            .select(*partition_columns)
            .distinct()
            .collect()
        )

        for row in affected_rows:

            partition_values = []

            for column in partition_columns:
                value = row[column]
                partition_values.append(
                    f"{column}={value}"
                )

            partition_path = (
                    target_path
                    + "/".join(partition_values)
                    + "/"
            )

            logger.info(
                f"Deleting affected Gold partition: "
                f"{partition_path}"
            )

            delete_s3_prefix(partition_path)

    # ======================================================
    # WRITE USING GLUE SINK
    # This writes Parquet AND updates Glue Data Catalog
    # ======================================================

    dynamic_frame = DynamicFrame.fromDF(
        df,
        glueContext,
        table_name
    )

    sink = glueContext.getSink(
        connection_type="s3",
        path=target_path,
        enableUpdateCatalog=True,
        updateBehavior="UPDATE_IN_DATABASE",
        partitionKeys=partition_columns,
        compression="snappy"
    )

    sink.setCatalogInfo(
        catalogDatabase=gold_database,
        catalogTableName=table_name
    )

    sink.setFormat(
        "parquet",
        useGlueParquetWriter=True
    )

    sink.writeFrame(dynamic_frame)

    logger.info(
        f"{table_name}: writeFrame completed. "
        f"Verifying S3 output..."
    )

    try:

        verification_df = spark.read.parquet(
            target_path
        )

        verification_count = verification_df.count()

        logger.info(
            f"{table_name}: S3 verification row count = "
            f"{verification_count}"
        )

        logger.info(
            f"{table_name}: S3 verification schema:"
        )

        verification_df.printSchema()

    except Exception as e:

        logger.exception(
            f"{table_name}: S3 verification FAILED"
        )

        raise

    logger.info(
        f"{table_name}: S3 write and Glue Catalog "
        f"update completed."
    )
# ==========================================================
# GOLD INCREMENTAL UPSERT
# ==========================================================

def upsert_gold_table(
    new_df,
    table_name,
    key_columns,
    partition_columns,
    deleted_keys_df=None,
    affected_partitions_df=None,
    is_bucket_partitioned=True
):

    try:

        target_path = GOLD_TABLE_PATHS[table_name]

        logger.info(
            f"Starting Gold upsert: {table_name}"
        )

        # ==================================================
        # FULL LOAD
        # ==================================================

        if LOAD_MODE == "FULL":

            logger.info(
                f"{table_name}: Initial full load."
            )

            delete_s3_prefix(
                target_path
            )

            write_to_gold_catalog(
                new_df,
                table_name,
                target_path,
                partition_columns
            )

            logger.info(
                f"{table_name}: Full load completed."
            )

            return

        # ==================================================
        # INCREMENTAL LOAD
        # ==================================================

        if new_df is None:

            # --------------------------------------------------
            # Create empty dataframe using existing Gold schema
            # --------------------------------------------------

            new_df = spark.createDataFrame(
                [],
                spark.read.parquet(target_path).schema
            )

        elif new_df.rdd.isEmpty():

            logger.info(
                f"{table_name}: New dataframe is empty."
            )

        # --------------------------------------------------
        # Partitions affected by new / updated records
        # --------------------------------------------------

        affected_partitions = (
            new_df
            .select(*partition_columns)
            .distinct()
        )

        # --------------------------------------------------
        # Add explicitly supplied affected partitions
        #
        # Example:
        # fact_sales_daily
        # DELETE order -> purchase_date affected
        # --------------------------------------------------

        if (
            affected_partitions_df is not None
            and not affected_partitions_df.rdd.isEmpty()
        ):

            affected_partitions = (
                affected_partitions
                .unionByName(
                    affected_partitions_df
                    .select(*partition_columns)
                )
                .distinct()
            )

        # --------------------------------------------------
        # Add partitions affected by deleted keys
        #
        # Used for bucket-partitioned Gold tables
        # --------------------------------------------------

        if (
                is_bucket_partitioned
                and deleted_keys_df is not None
                and not deleted_keys_df.rdd.isEmpty()
        ):
            deleted_partition_df = (
                deleted_keys_df
                .withColumn(
                    "_bucket",
                    F.pmod(
                        F.abs(
                            F.hash(
                                *[
                                    F.col(key).cast("string")
                                    for key in key_columns
                                ]
                            )
                        ),
                        F.lit(32)
                    )
                )
                .select(
                    "_bucket"
                )
                .distinct()
            )

            affected_partitions = (
                affected_partitions
                .unionByName(
                    deleted_partition_df
                )
                .distinct()
            )

        logger.info(
            f"{table_name}: Rebuilding affected "
            f"Gold partitions."
        )

        # ==================================================
        # READ EXISTING GOLD ONLY FOR AFFECTED PARTITIONS
        # ==================================================

        affected_partition_rows = (
            affected_partitions
            .collect()
        )

        if not affected_partition_rows:

            logger.info(
                f"{table_name}: No affected Gold partitions."
            )

            return

        # ==================================================
        # BUILD EXACT GOLD PARTITION PATHS
        # ==================================================

        partition_paths = []

        for row in affected_partition_rows:

            partition_values = []

            for column in partition_columns:

                value = row[column]

                partition_values.append(
                    f"{column}={value}"
                )

            partition_path = (
                target_path
                + "/".join(partition_values)
            )

            partition_paths.append(
                partition_path
            )

        logger.info(
            f"{table_name}: Reading only affected "
            f"Gold partitions: {partition_paths}"
        )

        # ==================================================
        # READ EXISTING GOLD PARTITIONS
        # ==================================================

        try:

            existing_df = (
                spark.read
                .parquet(*partition_paths)
            )

        except Exception:

            logger.warning(
                f"{table_name}: No existing Gold "
                f"partitions found."
            )

            existing_df = None

        # ==================================================
        # REBUILD AFFECTED PARTITIONS
        # ==================================================

        if existing_df is None:

            rebuilt_df = new_df

        else:

            existing_affected = existing_df

            # --------------------------------------------------
            # Remove old versions of records being replaced
            # --------------------------------------------------

            new_keys = (
                new_df
                .select(*key_columns)
                .distinct()
            )

            existing_unchanged = (
                existing_affected
                .alias("existing")
                .join(
                    new_keys.alias("new_keys"),
                    key_columns,
                    "left_anti"
                )
            )

            # --------------------------------------------------
            # Remove deleted records
            # --------------------------------------------------

            if (
                deleted_keys_df is not None
                and not deleted_keys_df.rdd.isEmpty()
            ):

                deleted_keys = (
                    deleted_keys_df
                    .select(*key_columns)
                    .distinct()
                )

                existing_unchanged = (
                    existing_unchanged
                    .alias("existing")
                    .join(
                        deleted_keys.alias("deleted"),
                        key_columns,
                        "left_anti"
                    )
                )

            # --------------------------------------------------
            # Rebuild affected partitions
            # --------------------------------------------------

            rebuilt_df = (
                existing_unchanged
                .unionByName(
                    new_df,
                    allowMissingColumns=True
                )
            )

        # ==================================================
        # DYNAMIC PARTITION OVERWRITE
        # ==================================================

        write_to_gold_catalog(
            rebuilt_df,
            table_name,
            target_path,
            partition_columns,
            affected_partitions_df=affected_partitions
        )

        logger.info(
            f"{table_name}: Incremental Gold "
            f"upsert completed."
        )

    except Exception:

        logger.exception(
            f"Gold upsert failed for {table_name}"
        )

        raise

# ==========================================================
# DELETED INVENTORY SUMMARY KEYS
# ==========================================================

deleted_inventory_summary_keys_df = None

if LOAD_MODE == "INCREMENTAL":

    # ==========================================================
    # DELETED INVENTORY SUMMARY KEYS
    # ==========================================================

    deleted_inventory_summary_keys_df = None

    if LOAD_MODE == "INCREMENTAL":
        deleted_inventory_ids_df = (
            changed_inventory_df
            .filter(
                F.col("operation") == "DELETE"
            )
            .select(
                "inventory_id"
            )
            .distinct()
        )

        # Read the OLD Gold inventory dimension so that
        # inventory_id can still be mapped to warehouse + product
        # after the Silver inventory record has been deleted.

        existing_gold_inventory = (
            spark.read
            .parquet(
                GOLD_TABLE_PATHS["dim_inventory"]
            )
        )

        deleted_inventory_summary_keys_df = (
            existing_gold_inventory
            .join(
                deleted_inventory_ids_df,
                "inventory_id",
                "inner"
            )
            .select(
                "warehouse_id",
                "product_id"
            )
            .distinct()
        )

# ==========================================================
# CLASSIFY GOLD UPSERT OPERATIONS
# ==========================================================

def classify_gold_changes(
    new_df,
    table_name,
    key_columns
):
    """
    Determines whether an affected Gold record is:
        INSERT
        UPDATE

    by checking whether the business key already exists
    in the previous Gold state.
    """

    try:

        if new_df is None or new_df.rdd.isEmpty():

            return None, None

        target_path = GOLD_TABLE_PATHS[table_name]

        try:

            existing_df = (
                spark.read
                .parquet(target_path)
                .select(*key_columns)
                .dropDuplicates()
            )

        except Exception:

            logger.info(
                f"{table_name}: Existing Gold state not found. "
                f"All records treated as INSERT."
            )

            return (
                new_df,
                spark.createDataFrame(
                    [],
                    new_df.schema
                )
            )

        # --------------------------------------------------
        # INSERT = key does not exist in previous Gold
        # --------------------------------------------------

        insert_df = (
            new_df
            .alias("new")
            .join(
                existing_df.alias("old"),
                key_columns,
                "left_anti"
            )
        )

        # --------------------------------------------------
        # UPDATE = key already existed
        # --------------------------------------------------

        update_df = (
            new_df
            .alias("new")
            .join(
                existing_df.alias("old"),
                key_columns,
                "inner"
            )
            .select("new.*")
        )

        return insert_df, update_df

    except Exception:

        logger.exception(
            f"Failed to classify Gold changes "
            f"for {table_name}"
        )

        raise

# ==========================================================
# WRITE GOLD CHANGE FEED
# ==========================================================

def write_gold_change_feed(
    df,
    table_name,
    key_columns,
    operation,
    pipeline_run_id,
    changed_at
):
    """
    Writes only records affected by the current Gold ETL run.

    This dataset is NOT the current Gold state.

    It is the incremental change feed consumed by Snowflake.

    operation:
        INSERT
        UPDATE
        DELETE
    """

    try:

        logger.info(
            f"Preparing Gold change feed for {table_name} "
            f"operation={operation}"
        )

        # --------------------------------------------------
        # Handle empty dataframe
        # --------------------------------------------------

        if df is None or df.rdd.isEmpty():

            logger.info(
                f"No {operation} changes for {table_name}."
            )

            return

        # --------------------------------------------------
        # Add CDC metadata
        # --------------------------------------------------

        change_df = (
            df
            .withColumn(
                "operation",
                F.lit(operation)
            )
            .withColumn(
                "pipeline_run_id",
                F.lit(pipeline_run_id)
            )
            .withColumn(
                "changed_at",
                F.lit(changed_at).cast("timestamp")
            )
        )

        # --------------------------------------------------
        # Remove duplicate business keys
        # --------------------------------------------------

        change_df = (
            change_df
            .dropDuplicates(key_columns)
        )

        # --------------------------------------------------
        # Write change batch
        #
        # Separate path per table and pipeline run.
        # Never overwrite previous change batches.
        # --------------------------------------------------

        output_path = (
            f"{GOLD_CHANGE_PATH}"
            f"{table_name}/"
            f"pipeline_run_id={pipeline_run_id}/"
        )

        (
            change_df
            .write
            .mode("append")
            .partitionBy("operation")
            .parquet(output_path)
        )

        logger.info(
            f"Gold change feed written successfully: "
            f"{table_name} | "
            f"{operation} | "
            f"rows={change_df.count()}"
        )

    except Exception:

        logger.exception(
            f"Failed to write Gold change feed "
            f"for {table_name} "
            f"operation={operation}"
        )

        raise

# ==========================================================
# WRITE GOLD TABLES
# ==========================================================

# ==========================================================
# CLASSIFY GOLD CHANGES BEFORE GOLD UPSERT
# ==========================================================
#
# IMPORTANT:
# This MUST happen BEFORE upsert_gold_table().
# Otherwise the existing Gold state would already contain
# the new records and everything would appear as UPDATE.
# ==========================================================

if LOAD_MODE == "INCREMENTAL":

    logger.info(
        "Classifying Gold records as INSERT or UPDATE "
        "before Gold upsert."
    )

    # ------------------------------------------------------
    # CUSTOMER
    # ------------------------------------------------------
    print("========== EXISTING GOLD CUSTOMER KEYS ==========")

    existing_customer_ids = (
        spark.read
        .parquet(GOLD_TABLE_PATHS["dim_customer"])
        .select("customer_id")
        .dropDuplicates()
    )

    existing_customer_ids.show(20, truncate=False)

    print("========== CURRENT INCREMENTAL CUSTOMER KEYS ==========")

    gold_dim_customer.select("customer_id").show(20, truncate=False)

    customer_insert_df, customer_update_df = (
        classify_gold_changes(
            gold_dim_customer,
            "dim_customer",
            ["customer_id"]
        )
    )

    # ------------------------------------------------------
    # PRODUCT
    # ------------------------------------------------------

    product_insert_df, product_update_df = (
        classify_gold_changes(
            gold_dim_product,
            "dim_product",
            ["product_id"]
        )
    )

    # ------------------------------------------------------
    # SELLER
    # ------------------------------------------------------

    seller_insert_df, seller_update_df = (
        classify_gold_changes(
            gold_dim_seller,
            "dim_seller",
            ["seller_id"]
        )
    )

    # ------------------------------------------------------
    # INVENTORY
    # ------------------------------------------------------

    inventory_insert_df, inventory_update_df = (
        classify_gold_changes(
            gold_dim_inventory,
            "dim_inventory",
            ["inventory_id"]
        )
    )

    # ------------------------------------------------------
    # FACT SALES
    # ------------------------------------------------------

    fact_sales_insert_df, fact_sales_update_df = (
        classify_gold_changes(
            gold_fact_sales,
            "fact_sales",
            [
                "order_id",
                "order_item_id"
            ]
        )
    )

    # ------------------------------------------------------
    # FACT PAYMENTS
    # ------------------------------------------------------

    fact_payments_insert_df, fact_payments_update_df = (
        classify_gold_changes(
            gold_fact_payments,
            "fact_payments",
            ["order_id"]
        )
    )

    # ------------------------------------------------------
    # SALES DAILY
    # ------------------------------------------------------

    sales_daily_insert_df, sales_daily_update_df = (
        classify_gold_changes(
            gold_sales_daily,
            "fact_sales_daily",
            ["purchase_date"]
        )
    )

    # ------------------------------------------------------
    # INVENTORY SUMMARY
    # ------------------------------------------------------

    inventory_summary_insert_df, inventory_summary_update_df = (
        classify_gold_changes(
            gold_inventory_summary,
            "inventory_summary",
            [
                "warehouse_id",
                "product_id"
            ]
        )
    )

    logger.info("Gold INSERT / UPDATE classification completed.")

else:

    # FULL LOAD = everything is INSERT
    customer_insert_df = gold_dim_customer
    customer_update_df = None

    product_insert_df = gold_dim_product
    product_update_df = None

    seller_insert_df = gold_dim_seller
    seller_update_df = None

    inventory_insert_df = gold_dim_inventory
    inventory_update_df = None

    fact_sales_insert_df = gold_fact_sales
    fact_sales_update_df = None

    fact_payments_insert_df = gold_fact_payments
    fact_payments_update_df = None

    sales_daily_insert_df = gold_sales_daily
    sales_daily_update_df = None

    inventory_summary_insert_df = gold_inventory_summary
    inventory_summary_update_df = None

upsert_gold_table(
    gold_dim_customer,
    "dim_customer",
    ["customer_id"],
    ["_bucket"],
    deleted_customers_df
    if LOAD_MODE == "INCREMENTAL"
    else None
)

upsert_gold_table(
    gold_dim_product,
    "dim_product",
    ["product_id"],
    ["_bucket"],
    deleted_products_df
    if LOAD_MODE == "INCREMENTAL"
    else None
)

upsert_gold_table(
    gold_dim_seller,
    "dim_seller",
    ["seller_id"],
    ["_bucket"],
    deleted_sellers_df
    if LOAD_MODE == "INCREMENTAL"
    else None
)

upsert_gold_table(
    gold_dim_inventory,
    "dim_inventory",
    ["inventory_id"],
    ["_bucket"],
    deleted_inventory_df
    if LOAD_MODE == "INCREMENTAL"
    else None
)

upsert_gold_table(
    gold_fact_sales,
    "fact_sales",
    ["order_id", "order_item_id"],
    ["_bucket"],
    deleted_fact_sales_keys_df
    if LOAD_MODE == "INCREMENTAL"
    else None
)

upsert_gold_table(
    gold_fact_payments,
    "fact_payments",
    ["order_id"],
    ["_bucket"],
    deleted_orders_df
    if LOAD_MODE == "INCREMENTAL"
    else None
)

upsert_gold_table(
    gold_sales_daily,
    "fact_sales_daily",
    ["purchase_date"],
    [
        "purchase_year",
        "purchase_month"
    ],
    deleted_keys_df=(
        changed_dates
        .select("purchase_date")
        .distinct()
        if LOAD_MODE == "INCREMENTAL"
        else None
    ),
    affected_partitions_df=(
        changed_dates
        if LOAD_MODE == "INCREMENTAL"
        else None
    ),
    is_bucket_partitioned=False
)

# ==========================================================
# INVENTORY SUMMARY
# ==========================================================
logger.info(
    "========== INVENTORY SUMMARY BEFORE WRITE =========="
)

logger.info(
    f"inventory_summary columns = {gold_inventory_summary.columns}"
)

logger.info(
    f"inventory_summary schema = {gold_inventory_summary.schema.simpleString()}"
)

logger.info(
    f"inventory_summary row count before write = "
    f"{gold_inventory_summary.count()}"
)

gold_inventory_summary.select(
    "warehouse_id",
    "warehouse_city",
    "warehouse_state",
    "product_id",
    "total_available_stock",
    "total_reserved_stock",
    "total_safety_stock",
    "reorder_level",
    "inventory_status",
    "last_updated",
    "_bucket"
).show(5, truncate=False)

logger.info(
    "===================================================="
)

upsert_gold_table(
    gold_inventory_summary,
    "inventory_summary",
    [
        "warehouse_id",
        "product_id"
    ],
    ["_bucket"],
    deleted_inventory_summary_keys_df
    if LOAD_MODE == "INCREMENTAL"
    else None
)

# ==========================================================
# GOLD CHANGE FEED FOR SNOWFLAKE
# ==========================================================

pipeline_run_id = (
    f"GOLD_{datetime.utcnow().strftime('%Y%m%d_%H%M%S_%f')}"
)

change_timestamp = datetime.utcnow()

logger.info(
    f"Starting Gold change feed generation. "
    f"Pipeline Run ID = {pipeline_run_id}"
)


# ==========================================================
# CUSTOMER CHANGE FEED
# ==========================================================

write_gold_change_feed(
    customer_insert_df,
    "dim_customer",
    ["customer_id"],
    "INSERT",
    pipeline_run_id,
    change_timestamp
)

write_gold_change_feed(
    customer_update_df,
    "dim_customer",
    ["customer_id"],
    "UPDATE",
    pipeline_run_id,
    change_timestamp
)

if LOAD_MODE == "INCREMENTAL":

    write_gold_change_feed(
        deleted_customers_df,
        "dim_customer",
        ["customer_id"],
        "DELETE",
        pipeline_run_id,
        change_timestamp
    )


# ==========================================================
# PRODUCT CHANGE FEED
# ==========================================================

write_gold_change_feed(
    product_insert_df,
    "dim_product",
    ["product_id"],
    "INSERT",
    pipeline_run_id,
    change_timestamp
)

write_gold_change_feed(
    product_update_df,
    "dim_product",
    ["product_id"],
    "UPDATE",
    pipeline_run_id,
    change_timestamp
)

if LOAD_MODE == "INCREMENTAL":

    write_gold_change_feed(
        deleted_products_df,
        "dim_product",
        ["product_id"],
        "DELETE",
        pipeline_run_id,
        change_timestamp
    )


# ==========================================================
# SELLER CHANGE FEED
# ==========================================================

write_gold_change_feed(
    seller_insert_df,
    "dim_seller",
    ["seller_id"],
    "INSERT",
    pipeline_run_id,
    change_timestamp
)

write_gold_change_feed(
    seller_update_df,
    "dim_seller",
    ["seller_id"],
    "UPDATE",
    pipeline_run_id,
    change_timestamp
)

if LOAD_MODE == "INCREMENTAL":

    write_gold_change_feed(
        deleted_sellers_df,
        "dim_seller",
        ["seller_id"],
        "DELETE",
        pipeline_run_id,
        change_timestamp
    )


# ==========================================================
# INVENTORY CHANGE FEED
# ==========================================================

write_gold_change_feed(
    inventory_insert_df,
    "dim_inventory",
    ["inventory_id"],
    "INSERT",
    pipeline_run_id,
    change_timestamp
)

write_gold_change_feed(
    inventory_update_df,
    "dim_inventory",
    ["inventory_id"],
    "UPDATE",
    pipeline_run_id,
    change_timestamp
)

if LOAD_MODE == "INCREMENTAL":

    write_gold_change_feed(
        deleted_inventory_df,
        "dim_inventory",
        ["inventory_id"],
        "DELETE",
        pipeline_run_id,
        change_timestamp
    )


# ==========================================================
# FACT SALES CHANGE FEED
# ==========================================================

write_gold_change_feed(
    fact_sales_insert_df,
    "fact_sales",
    [
        "order_id",
        "order_item_id"
    ],
    "INSERT",
    pipeline_run_id,
    change_timestamp
)

write_gold_change_feed(
    fact_sales_update_df,
    "fact_sales",
    [
        "order_id",
        "order_item_id"
    ],
    "UPDATE",
    pipeline_run_id,
    change_timestamp
)

if LOAD_MODE == "INCREMENTAL":

    write_gold_change_feed(
        deleted_fact_sales_keys_df,
        "fact_sales",
        [
            "order_id",
            "order_item_id"
        ],
        "DELETE",
        pipeline_run_id,
        change_timestamp
    )


# ==========================================================
# FACT PAYMENTS CHANGE FEED
# ==========================================================

write_gold_change_feed(
    fact_payments_insert_df,
    "fact_payments",
    ["order_id"],
    "INSERT",
    pipeline_run_id,
    change_timestamp
)

write_gold_change_feed(
    fact_payments_update_df,
    "fact_payments",
    ["order_id"],
    "UPDATE",
    pipeline_run_id,
    change_timestamp
)

if LOAD_MODE == "INCREMENTAL":

    write_gold_change_feed(
        deleted_orders_df,
        "fact_payments",
        ["order_id"],
        "DELETE",
        pipeline_run_id,
        change_timestamp
    )


# ==========================================================
# FACT SALES DAILY CHANGE FEED
# ==========================================================

write_gold_change_feed(
    sales_daily_insert_df,
    "fact_sales_daily",
    ["purchase_date"],
    "INSERT",
    pipeline_run_id,
    change_timestamp
)

write_gold_change_feed(
    sales_daily_update_df,
    "fact_sales_daily",
    ["purchase_date"],
    "UPDATE",
    pipeline_run_id,
    change_timestamp
)


# ==========================================================
# INVENTORY SUMMARY CHANGE FEED
# ==========================================================

write_gold_change_feed(
    inventory_summary_insert_df,
    "inventory_summary",
    [
        "warehouse_id",
        "product_id"
    ],
    "INSERT",
    pipeline_run_id,
    change_timestamp
)

write_gold_change_feed(
    inventory_summary_update_df,
    "inventory_summary",
    [
        "warehouse_id",
        "product_id"
    ],
    "UPDATE",
    pipeline_run_id,
    change_timestamp
)

if LOAD_MODE == "INCREMENTAL":

    write_gold_change_feed(
        deleted_inventory_summary_keys_df,
        "inventory_summary",
        [
            "warehouse_id",
            "product_id"
        ],
        "DELETE",
        pipeline_run_id,
        change_timestamp
    )


logger.info(
    "Gold change feed generation completed successfully."
)

# ==========================================================
# CALCULATE NEW GOLD WATERMARK
# ==========================================================

try:

    if LOAD_MODE == "FULL":

        new_watermark = datetime.utcnow()

        logger.info(
            f"Full Gold load completed. "
            f"Setting watermark to {new_watermark}"
        )

    else:

        new_watermark = (
            silver_changes_df
            .select(
                F.max("changed_at")
                .alias("max_changed_at")
            )
            .collect()[0]["max_changed_at"]
        )

        if new_watermark is None:

            new_watermark = gold_watermark

            logger.info(
                "No new Silver changes detected. "
                "Gold watermark remains unchanged."
            )

    logger.info(
        f"New Gold watermark: {new_watermark}"
    )

except Exception:

    logger.exception(
        "Failed to calculate Gold watermark."
    )

    raise


# ==========================================================
# SAVE WATERMARK ONLY AFTER GOLD LOAD SUCCEEDS
# ==========================================================

save_gold_watermark(
    new_watermark
)


# ==========================================================
# COMPLETE
# ==========================================================

logger.info(
    "=========================================="
)

logger.info(
    "Gold ETL Completed Successfully."
)

logger.info(
    f"Load Mode : {LOAD_MODE}"
)

logger.info(
    f"Gold Watermark : {new_watermark}"
)

logger.info(
    "=========================================="
)

job.commit()