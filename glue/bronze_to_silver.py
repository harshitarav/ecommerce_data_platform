import sys

from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.sql.functions import count,col
from pyspark.sql import functions as F

from common.logger import ETLLogger

from common.silver_utils import (
    remove_soft_deleted,
    remove_duplicates,
    trim_string_columns,
    handle_null_values,
    standardize_values
)

# Initialize Spark and Glue Context
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

job = Job(glueContext)
job.init(args['JOB_NAME'], args)

SILVER_BASE_PATH = "s3://e-commerce-de-project/silver"

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


        bronze_dynamic_frame = glueContext.create_dynamic_frame.from_catalog(
            database="bronze_db",
            table_name="customers"
        )

        customers_df = bronze_dynamic_frame.toDF()
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
        customers_df = remove_soft_deleted(customers_df)
        count_after_soft_delete = customers_df.count()

        logger.info(
            event="REMOVE_SOFT_DELETED",
            message="Soft deleted records removed",
            rows_after_soft_delete=count_after_soft_delete
        )


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
        customers_df.write \
            .mode("overwrite") \
            .parquet(f"{SILVER_BASE_PATH}/customers/")

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

        bronze_dynamic_frame = glueContext.create_dynamic_frame.from_catalog(
            database="bronze_db",
            table_name="orders"
        )

        orders_df = bronze_dynamic_frame.toDF()

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

        orders_df = remove_soft_deleted(orders_df)

        count_after_soft_delete = orders_df.count()

        logger.info(
            event="REMOVE_SOFT_DELETED",
            message="Soft deleted records removed",
            rows_after_soft_delete=count_after_soft_delete
        )

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

        orders_df.write \
            .mode("overwrite") \
            .parquet(f"{SILVER_BASE_PATH}/orders/")

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

        bronze_dynamic_frame = glueContext.create_dynamic_frame.from_catalog(
            database="bronze_db",
            table_name="order_items"
        )

        order_items_df = bronze_dynamic_frame.toDF()

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

        order_items_df = remove_soft_deleted(order_items_df)
        count_after_soft_delete = order_items_df.count()

        logger.info(
            event="REMOVE_SOFT_DELETED",
            message="Soft deleted records removed",
            rows_after_soft_delete=count_after_soft_delete
        )

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

        order_items_df.write \
            .mode("overwrite") \
            .parquet(f"{SILVER_BASE_PATH}/order_items/")

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

        bronze_dynamic_frame = glueContext.create_dynamic_frame.from_catalog(
            database="bronze_db",
            table_name="products"
        )

        products_df = bronze_dynamic_frame.toDF()

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

        products_df = remove_soft_deleted(products_df)

        count_after_soft_delete = products_df.count()

        logger.info(
            event="REMOVE_SOFT_DELETED",
            message="Soft deleted records removed",
            rows_after_soft_delete=count_after_soft_delete
        )

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

        products_df.write \
            .mode("overwrite") \
            .parquet(f"{SILVER_BASE_PATH}/products/")

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

        bronze_dynamic_frame = glueContext.create_dynamic_frame.from_catalog(
            database="bronze_db",
            table_name="payments"
        )

        payments_df = bronze_dynamic_frame.toDF()

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

        payments_df = remove_soft_deleted(payments_df)

        count_after_soft_delete = payments_df.count()

        logger.info(
            event="REMOVE_SOFT_DELETED",
            message="Soft deleted records removed",
            rows_after_soft_delete=count_after_soft_delete
        )

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

        payments_df.write \
            .mode("overwrite") \
            .parquet(f"{SILVER_BASE_PATH}/payments/")

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

        bronze_dynamic_frame = glueContext.create_dynamic_frame.from_catalog(
            database="bronze_db",
            table_name="sellers"
        )

        sellers_df = bronze_dynamic_frame.toDF()

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

        sellers_df = remove_soft_deleted(sellers_df)

        count_after_soft_delete = sellers_df.count()

        logger.info(
            event="REMOVE_SOFT_DELETED",
            message="Soft deleted records removed",
            rows_after_soft_delete=count_after_soft_delete
        )

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

        sellers_df.write \
            .mode("overwrite") \
            .parquet(f"{SILVER_BASE_PATH}/sellers/")

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

        bronze_dynamic_frame = glueContext.create_dynamic_frame.from_catalog(
            database="bronze_db",
            table_name="reviews"
        )

        reviews_df = bronze_dynamic_frame.toDF()

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

        reviews_df = remove_soft_deleted(reviews_df)

        count_after_soft_delete = reviews_df.count()

        logger.info(
            event="REMOVE_SOFT_DELETED",
            message="Soft deleted records removed",
            rows_after_soft_delete=count_after_soft_delete
        )

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

        reviews_df.write \
            .mode("overwrite") \
            .parquet(f"{SILVER_BASE_PATH}/reviews/")

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

        bronze_dynamic_frame = glueContext.create_dynamic_frame.from_catalog(
            database="bronze_db",
            table_name="inventory_initial_production_v2"
        )

        inventory_df = bronze_dynamic_frame.toDF()

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

        inventory_df = remove_soft_deleted(inventory_df)

        count_after_soft_delete = inventory_df.count()

        logger.info(
            event="REMOVE_SOFT_DELETED",
            message="Soft deleted records removed",
            rows_after_soft_delete=count_after_soft_delete
        )

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

        inventory_df.write \
            .mode("overwrite") \
            .parquet(f"{SILVER_BASE_PATH}/inventory/")

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

        bronze_dynamic_frame = glueContext.create_dynamic_frame.from_catalog(
            database="bronze_db",
            table_name="shipment_management_system"
        )

        shipment_df = bronze_dynamic_frame.toDF()

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

        shipment_df = remove_soft_deleted(shipment_df)

        count_after_soft_delete = shipment_df.count()

        logger.info(
            event="REMOVE_SOFT_DELETED",
            message="Soft deleted records removed",
            rows_after_soft_delete=count_after_soft_delete
        )

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

        shipment_df.write \
            .mode("overwrite") \
            .parquet(f"{SILVER_BASE_PATH}/shipment/")

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

        bronze_dynamic_frame = glueContext.create_dynamic_frame.from_catalog(
            database="bronze_db",
            table_name="olist_geolocation_dataset"
        )

        geolocation_df = bronze_dynamic_frame.toDF()

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

        geolocation_df = remove_soft_deleted(geolocation_df)

        count_after_soft_delete = geolocation_df.count()

        logger.info(
            event="REMOVE_SOFT_DELETED",
            message="Soft deleted records removed",
            rows_after_soft_delete=count_after_soft_delete
        )

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

        geolocation_df.write \
            .mode("overwrite") \
            .parquet(f"{SILVER_BASE_PATH}/geolocation/")

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