import sys
import logging

from pyspark.context import SparkContext
from pyspark.sql import functions as F

from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from awsglue.dynamicframe import DynamicFrame
import boto3
from urllib.parse import urlparse
from pyspark.sql.window import Window

# ----------------------------------
# Job Initialization
# ----------------------------------

args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

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

# ----------------------------------
# Read Glue Catalog Table
# ----------------------------------

def read_table(database, table_name):

    logger.info(f"Reading {table_name}")

    dyf = glueContext.create_dynamic_frame.from_catalog(
        database=database,
        table_name=table_name
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

silver_db = "silver_db"

orders_df = read_table(silver_db, "orders")

order_items_df = read_table(silver_db, "order_items")

payments_df = read_table(silver_db, "payments")

reviews_df = read_table(silver_db, "reviews")

shipment_df = read_table(
    silver_db,
    "shipment"
)

customers_df = read_table(silver_db, "customers")

products_df = read_table(silver_db, "products")

sellers_df = read_table(silver_db, "sellers")

geolocation_df = read_table(
    silver_db,
    "geolocation"
)

inventory_df = read_table(
    silver_db,
    "inventory"
)

orders_df = orders_df.select(
    "order_id",
    "customer_id",
    "order_status",
    "order_purchase_timestamp",
    "order_approved_at",
    "order_delivered_customer_date",
    "order_estimated_delivery_date"
).alias("o")

order_items_df = order_items_df.select(
    "order_id",
    "order_item_id",
    "product_id",
    "seller_id",
    "shipping_limit_date",
    "price",
    "freight_value"
).alias("oi")

payments_df = payments_df.select(
    "order_id",
    "payment_type",
    "payment_installments",
    "payment_value"
).alias("p")

reviews_df = reviews_df.select(
    "order_id",
    "review_score",
    "review_creation_date",
    "review_answer_timestamp"
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
    "loyalty_points"
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
    "product_width_cm"
).alias("pr")

sellers_df = sellers_df.select(
    "seller_id",
    "seller_zip_code_prefix",
    "seller_city",
    "seller_state"
).alias("se")

geolocation_df = geolocation_df.select(
    "geolocation_zip_code_prefix",
    "geolocation_lat",
    "geolocation_lng",
    "geolocation_city",
    "geolocation_state"
).alias("g")

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
# Fact Payments
# Grain:
# One row = One Order
# Multiple payment methods are aggregated into a single record.
# ----------------------------------

# fact_payments_df = (
#     payments_df.join(
#         orders_df,
#         F.col("p.order_id") == F.col("o.order_id"),
#         "inner"
#     )
# )

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

        F.sum("available_stock").alias("total_available_stock"),

        F.sum("reserved_stock").alias("total_reserved_stock"),

        F.sum("safety_stock").alias("total_safety_stock"),

        F.max("reorder_level").alias("reorder_level"),

        F.first("inventory_status").alias("inventory_status"),

        F.max("last_updated").alias("last_updated")

    )

)

# ----------------------------------
# Write DataFrame to Gold S3
# ----------------------------------

gold_database = "gold_db"
gold_path = "s3://e-commerce-de-project/gold/"

def write_gold_table(
    df,
    table_name,
    partition_keys=None
):

    if partition_keys is None:
        partition_keys = []

    logger.info(f"Writing {table_name}")

    if table_name.startswith("dim_"):
        output_path = gold_path + "dimensions/" + table_name

    elif table_name in ["fact_sales", "fact_payments"]:
        output_path = gold_path + "facts/" + table_name

    else:
        output_path = gold_path + "marts/" + table_name

    delete_s3_prefix(output_path)

    dynamic_frame = DynamicFrame.fromDF(
        df,
        glueContext,
        table_name
    )

    sink = glueContext.getSink(
        connection_type="s3",
        path=output_path,
        enableUpdateCatalog=True,
        updateBehavior="UPDATE_IN_DATABASE",
        partitionKeys=partition_keys,
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

    try:
        sink.writeFrame(dynamic_frame)
        logger.info(f"{table_name} written successfully.")

    except Exception as e:
        logger.exception(f"Failed to write {table_name}")
        raise

# ----------------------------------
# Write Gold Dimensions
# ----------------------------------

write_gold_table(
    dim_customer_df,
    "dim_customer"
)

write_gold_table(
    dim_product_df,
    "dim_product"
)

write_gold_table(
    dim_seller_df,
    "dim_seller"
)

write_gold_table(
    dim_inventory_df,
    "dim_inventory"
)

write_gold_table(
    fact_sales_df,
    "fact_sales",
    ["purchase_year", "purchase_month"]
)

write_gold_table(
    fact_payments_df,
    "fact_payments",
    ["purchase_year", "purchase_month"]
)

write_gold_table(
    fact_sales_daily_df,
    "fact_sales_daily"
)

write_gold_table(
    inventory_summary_df,
    "inventory_summary"
)

logger.info("Gold ETL Completed Successfully.")

job.commit()