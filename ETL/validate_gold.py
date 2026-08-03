import sys
import logging

from pyspark.context import SparkContext
from pyspark.sql import functions as F

from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from utils.config_utils import load_validation_config
from utils.report_utils import ValidationReport

from utils.validation_utils import (
    check_duplicates,
    check_nulls,
    check_numeric_range,
    check_allowed_values,
    check_row_count
)

args = getResolvedOptions(
    sys.argv,
    ["JOB_NAME", "CONFIG_PATH"]
)

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

job = Job(glueContext)
job.init(args['JOB_NAME'], args)

config_path = args["CONFIG_PATH"]

validation_config = load_validation_config(config_path)

tables_config = validation_config["tables"]

business_rules = validation_config["business_rules"]

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s"
)

logger = logging.getLogger(__name__)

logger.info("Starting Gold Validation")

report = ValidationReport(
    spark=spark,
    pipeline_name="gold",
    job_name=args["JOB_NAME"],
    bucket_name="e-commerce-de-project"
)

def read_table(database, table_name):

    logger.info(f"Reading {table_name}")

    dyf = glueContext.create_dynamic_frame.from_catalog(
        database=database,
        table_name=table_name
    )

    return dyf.toDF()

def validate_duplicates(
    df,
    table_name,
    columns
):

    duplicate_count = check_duplicates(
        df,
        columns
    )

    status = (
        "PASS"
        if duplicate_count == 0
        else "FAIL"
    )

    report.add_result(

        validation_name=f"{table_name} Duplicate Check",

        status=status,

        severity="CRITICAL",

        expected=0,

        actual=duplicate_count,

        remarks=f"{columns} must be unique."

    )

    logger.info(
        f"{table_name} Duplicate Validation Completed."
    )

GOLD_DATABASE = "gold_db"

dim_customer_df = read_table(
    GOLD_DATABASE,
    "dim_customer"
)

dim_product_df = read_table(
    GOLD_DATABASE,
    "dim_product"
)

dim_seller_df = read_table(
    GOLD_DATABASE,
    "dim_seller"
)

dim_inventory_df = read_table(
    GOLD_DATABASE,
    "dim_inventory"
)

fact_sales_df = read_table(
    GOLD_DATABASE,
    "fact_sales"
)

fact_payments_df = read_table(
    GOLD_DATABASE,
    "fact_payments"
)

fact_sales_daily_df = read_table(
    GOLD_DATABASE,
    "fact_sales_daily"
)

inventory_summary_df = read_table(
    GOLD_DATABASE,
    "inventory_summary"
)

logger.info(
    "Successfully loaded all Gold tables."
)


def validate_row_count(df, table_name):

    row_count = df.count()

    status = (
        "PASS"
        if row_count > 0
        else "FAIL"
    )

    report.add_result(

        validation_name=f"{table_name} Row Count",

        status=status,

        severity="CRITICAL",

        expected="> 0",

        actual=row_count,

        remarks="Table should not be empty."

    )

    logger.info(
        f"{table_name} Row Count Validation Completed."
    )

#Null Validation
def validate_not_null(
    df,
    table_name,
    columns
):

    for column in columns:

        null_count = (

            df.filter(
                F.col(column).isNull()
            ).count()

        )

        status = (
            "PASS"
            if null_count == 0
            else "FAIL"
        )

        report.add_result(

            validation_name=f"{table_name}.{column} NULL Check",

            status=status,

            severity="CRITICAL",

            expected=0,

            actual=null_count,

            remarks=f"{column} should not contain NULL values."

        )

        logger.info(
            f"{table_name}.{column} NULL Validation Completed."
        )

try:
    #Row Count Validation
    validate_row_count(
        dim_customer_df,
        "dim_customer"
    )

    validate_row_count(
        dim_product_df,
        "dim_product"
    )

    validate_row_count(
        dim_seller_df,
        "dim_seller"
    )

    validate_row_count(
        dim_inventory_df,
        "dim_inventory"
    )

    validate_row_count(
        fact_sales_df,
        "fact_sales"
    )

    validate_row_count(
        fact_payments_df,
        "fact_payments"
    )

    validate_row_count(
        fact_sales_daily_df,
        "fact_sales_daily"
    )

    validate_row_count(
        inventory_summary_df,
        "inventory_summary"
    )

    validate_duplicates(
        dim_customer_df,
        "dim_customer",
        "customer_id"
    )

    validate_duplicates(
        dim_product_df,
        "dim_product",
        "product_id"
    )

    validate_duplicates(
        dim_seller_df,
        "dim_seller",
        "seller_id"
    )

    validate_duplicates(
        dim_inventory_df,
        "dim_inventory",
        "inventory_id"
    )

    validate_duplicates(
        fact_sales_df,
        "fact_sales",
        ["order_id", "order_item_id"]
    )

    validate_duplicates(
        fact_sales_daily_df,
        "fact_sales_daily",
        "purchase_date"
    )

    validate_duplicates(
        inventory_summary_df,
        "inventory_summary",
        ["warehouse_id", "product_id"]
    )

    #fact_sales null validation
    validate_not_null(
        fact_sales_df,
        "fact_sales",
        tables_config["fact_sales"]["not_null_columns"]
    )

    #fact_payments null validation
    validate_not_null(
        fact_payments_df,
        "fact_payments",
        tables_config["fact_payments"]["not_null_columns"]
    )

    #dim_customer null validation
    validate_not_null(
        dim_customer_df,
        "dim_customer",
        tables_config["dim_customer"]["not_null_columns"]
    )

    #dim_product null validation
    validate_not_null(
        dim_product_df,
        "dim_product",
        tables_config["dim_product"]["not_null_columns"]
    )

    #dim_seller null validation
    validate_not_null(
        dim_seller_df,
        "dim_seller",
        tables_config["dim_seller"]["not_null_columns"]
    )

    #dim_inventory null validation
    validate_not_null(
        dim_inventory_df,
        "dim_inventory",
        tables_config["dim_inventory"]["not_null_columns"]
    )

    # Business Rule Validations
    #Total Order Value Validation
    invalid_total_order_value = (

        fact_sales_df

        .filter(
            F.col("total_order_value") <
            business_rules["total_order_value"]["min"]
        )

        .count()

    )

    status = (
        "PASS"
        if invalid_total_order_value == 0
        else "FAIL"
    )

    report.add_result(

        validation_name="Total Order Value Validation",

        status=status,

        severity="ERROR",

        expected=">= 0",

        actual=invalid_total_order_value,

        remarks="total_order_value should never be negative."

    )

    logger.info(
        "Total Order Value validation completed."
    )

    # Payment Value Validation
    payment_value_min = business_rules["payment_value"]["min"]
    invalid_payment_value = (

        fact_payments_df

        .filter(
            F.col("payment_value") < payment_value_min
        )

        .count()

    )

    status = (
        "PASS"
        if invalid_payment_value == 0
        else "FAIL"
    )

    report.add_result(

        validation_name="Payment Value Validation",

        status=status,

        severity="ERROR",

        expected=">= 0",

        actual=invalid_payment_value,

        remarks="payment_value should never be negative."

    )

    if invalid_payment_value > 0:
        invalid_payment_df = (

            fact_payments_df

            .filter(
                F.col("payment_value") < payment_value_min
            )

        )

        logger.info(
            "Invalid Payment Records:"
        )

        invalid_payment_df.select(
            "order_id",
            "payment_value",
            "payment_type"
        ).show(
            20,
            truncate=False
        )

    logger.info(
        "Payment Value validation completed."
    )

    # Delivery Days Validation
    invalid_delivery_days = (

        fact_sales_df

        .filter(
            F.col("delivery_days") <
            business_rules["delivery_days"]["min"]
        )

        .count()

    )

    status = (
        "PASS"
        if invalid_delivery_days == 0
        else "FAIL"
    )

    report.add_result(

        validation_name="Delivery Days Validation",

        status=status,

        severity="ERROR",

        expected=">= 0",

        actual=invalid_delivery_days,

        remarks="delivery_days should never be negative."

    )

    logger.info(
        "Delivery Days validation completed."
    )

    # Purchase Year Validation
    purchase_year_min = business_rules["purchase_year"]["min"]
    purchase_year_max = business_rules["purchase_year"]["max"]
    invalid_purchase_year = (

        fact_sales_df

        .filter(
            (F.col("purchase_year") < purchase_year_min) |

            (F.col("purchase_year") > purchase_year_max)
        )

        .count()

    )

    status = (
        "PASS"
        if invalid_purchase_year == 0
        else "FAIL"
    )

    report.add_result(

        validation_name="Purchase Year Validation",

        status=status,

        severity="ERROR",

        expected=f"{purchase_year_min}-{purchase_year_max}",

        actual=invalid_purchase_year,

        remarks="purchase_year should be within the configured range."

    )

    logger.info(
        "Purchase Year validation completed."
    )

    # Review Score Validation
    invalid_review_score = (

        fact_sales_df

        .filter(

            (~F.col("review_score").isin(*business_rules["review_score"]["allowed_values"])) &

            (F.col("review_score").isNotNull())

        )

        .count()

    )

    status = (
        "PASS"
        if invalid_review_score == 0
        else "FAIL"
    )

    report.add_result(

        validation_name="Review Score Validation",

        status=status,

        severity="ERROR",

        expected=str(
            business_rules["review_score"]["allowed_values"]
        ),

        actual=invalid_review_score,

        remarks="review_score must contain only allowed values."

    )

    logger.info(
        "Review Score validation completed."
    )

    # Late Delivery Flag Validation
    invalid_delivery_flag = (

        fact_sales_df

        .filter(

            ~F.col("late_delivery_flag").isin(*business_rules["late_delivery_flag"]["allowed_values"])

        )

        .count()

    )

    status = (
        "PASS"
        if invalid_delivery_flag == 0
        else "FAIL"
    )

    report.add_result(

        validation_name="Late Delivery Flag Validation",

        status=status,

        severity="WARNING",

        expected=str(
            business_rules["late_delivery_flag"]["allowed_values"]
        ),

        actual=invalid_delivery_flag,

        remarks="late_delivery_flag must contain only allowed values."

    )

    logger.info(
        "Late Delivery Flag validation completed."
    )

    # Review Category Validation
    invalid_review_category = (

        fact_sales_df

        .filter(

            ~F.col("review_category").isin(*business_rules["review_category"]["allowed_values"])

        )

        .count()

    )

    status = (
        "PASS"
        if invalid_review_category == 0
        else "FAIL"
    )

    report.add_result(

        validation_name="Review Category Validation",

        status=status,

        severity="WARNING",

        expected=str(
            business_rules["review_category"]["allowed_values"]
        ),

        actual=invalid_review_category,

        remarks="review_category must contain only allowed values."

    )

    logger.info(
        "Review Category validation completed."
    )

    # Total Sales Aggregation Validation
    fact_sales_total = (

        fact_sales_df

        .agg(
            F.sum("total_order_value").alias("total_sales")
        )

        .collect()[0]["total_sales"]

    )

    fact_sales_daily_total = (

        fact_sales_daily_df

        .agg(
            F.sum("total_sales").alias("total_sales")
        )

        .collect()[0]["total_sales"]

    )

    status = (
        "PASS"
        if round(fact_sales_total, 2) ==
           round(fact_sales_daily_total, 2)
        else "FAIL"
    )

    report.add_result(

        validation_name="Sales Aggregation Validation",

        status=status,

        severity="CRITICAL",

        expected=round(fact_sales_total, 2),

        actual=round(fact_sales_daily_total, 2),

        remarks="Fact Sales and Daily Sales totals must match."

    )

    logger.info(
        "Sales aggregation validation completed."
    )

    # Total Orders Aggregation Validation
    fact_sales_orders = (

        fact_sales_df

        .select("order_id")

        .distinct()

        .count()

    )

    fact_sales_daily_orders = (

        fact_sales_daily_df

        .agg(
            F.sum("total_orders").alias("total_orders")
        )

        .collect()[0]["total_orders"]

    )

    status = (
        "PASS"
        if fact_sales_orders ==
           fact_sales_daily_orders
        else "FAIL"
    )

    report.add_result(

        validation_name="Order Aggregation Validation",

        status=status,

        severity="CRITICAL",

        expected=fact_sales_orders,

        actual=fact_sales_daily_orders,

        remarks="Order totals must match."

    )

    logger.info(
        "Order aggregation validation completed."
    )

    # Total Items Sold Aggregation Validation
    fact_items_sold = (

        fact_sales_df

        .count()

    )

    fact_sales_daily_items = (

        fact_sales_daily_df

        .agg(
            F.sum("total_items_sold").alias("total_items")
        )

        .collect()[0]["total_items"]

    )

    status = (
        "PASS"
        if fact_items_sold ==
           fact_sales_daily_items
        else "FAIL"
    )

    report.add_result(

        validation_name="Items Sold Aggregation Validation",

        status=status,

        severity="CRITICAL",

        expected=fact_items_sold,

        actual=fact_sales_daily_items,

        remarks="Items Sold totals must match."

    )

    logger.info(
        "Items Sold aggregation validation completed."
    )

    # Total Freight Aggregation Validation
    fact_total_freight = (

        fact_sales_df

        .agg(
            F.sum("freight_value").alias("total_freight")
        )

        .collect()[0]["total_freight"]

    )

    fact_sales_daily_freight = (

        fact_sales_daily_df

        .agg(
            F.sum("total_freight").alias("total_freight")
        )

        .collect()[0]["total_freight"]

    )

    status = (
        "PASS"
        if round(fact_total_freight, 2) ==
           round(fact_sales_daily_freight, 2)
        else "FAIL"
    )

    report.add_result(

        validation_name="Freight Aggregation Validation",

        status=status,

        severity="CRITICAL",

        expected=round(fact_total_freight, 2),

        actual=round(fact_sales_daily_freight, 2),

        remarks="Freight totals must match."

    )

    logger.info(
        "Freight aggregation validation completed."
    )

    # Late Deliveries Aggregation Validation
    fact_late_deliveries = (

        fact_sales_df

        .filter(
            F.col("late_delivery_flag") == "Yes"
        )

        .count()

    )

    fact_sales_daily_late = (

        fact_sales_daily_df

        .agg(
            F.sum("late_deliveries").alias("late_deliveries")
        )

        .collect()[0]["late_deliveries"]

    )

    status = (
        "PASS"
        if fact_late_deliveries ==
           fact_sales_daily_late
        else "FAIL"
    )

    report.add_result(

        validation_name="Late Delivery Aggregation Validation",

        status=status,

        severity="CRITICAL",

        expected=fact_late_deliveries,

        actual=fact_sales_daily_late,

        remarks="Late Delivery totals must match."

    )

    logger.info(
        "Late Delivery aggregation validation completed."
    )

    # Available Stock Aggregation Validation
    inventory_available_stock = (

        dim_inventory_df

        .agg(
            F.sum("available_stock").alias("available_stock")
        )

        .collect()[0]["available_stock"]

    )

    inventory_summary_available = (

        inventory_summary_df

        .agg(
            F.sum("total_available_stock").alias("available_stock")
        )

        .collect()[0]["available_stock"]

    )

    status = (
        "PASS"
        if inventory_available_stock ==
           inventory_summary_available
        else "FAIL"
    )

    report.add_result(

        validation_name="Available Stock Aggregation Validation",

        status=status,

        severity="CRITICAL",

        expected=inventory_available_stock,

        actual=inventory_summary_available,

        remarks="Available Stock totals must match."

    )

    logger.info(
        "Available Stock aggregation validation completed."
    )

    # Reserved Stock Aggregation Validation
    inventory_reserved_stock = (

        dim_inventory_df

        .agg(
            F.sum("reserved_stock").alias("reserved_stock")
        )

        .collect()[0]["reserved_stock"]

    )

    inventory_summary_reserved = (

        inventory_summary_df

        .agg(
            F.sum("total_reserved_stock").alias("reserved_stock")
        )

        .collect()[0]["reserved_stock"]

    )

    status = (
        "PASS"
        if inventory_reserved_stock ==
           inventory_summary_reserved
        else "FAIL"
    )

    report.add_result(

        validation_name="Reserved Stock Aggregation Validation",

        status=status,

        severity="CRITICAL",

        expected=inventory_reserved_stock,

        actual=inventory_summary_reserved,

        remarks="Reserved Stock totals must match."

    )

    logger.info(
        "Reserved Stock aggregation validation completed."
    )

    # Safety Stock Aggregation Validation
    inventory_safety_stock = (

        dim_inventory_df

        .agg(
            F.sum("safety_stock").alias("safety_stock")
        )

        .collect()[0]["safety_stock"]

    )

    inventory_summary_safety = (

        inventory_summary_df

        .agg(
            F.sum("total_safety_stock").alias("safety_stock")
        )

        .collect()[0]["safety_stock"]

    )

    status = (
        "PASS"
        if inventory_safety_stock ==
           inventory_summary_safety
        else "FAIL"
    )

    report.add_result(

        validation_name="Safety Stock Aggregation Validation",

        status=status,

        severity="CRITICAL",

        expected=inventory_safety_stock,

        actual=inventory_summary_safety,

        remarks="Safety Stock totals must match."

    )

    logger.info(
        "Safety Stock aggregation validation completed."
    )

    # Cross tale validations
    # Customer Referential Integrity Validation
    invalid_customers = (

        fact_sales_df

        .join(
            dim_customer_df,
            "customer_id",
            "left_anti"
        )

        .count()

    )

    status = (
        "PASS"
        if invalid_customers == 0
        else "FAIL"
    )

    report.add_result(

        validation_name="Customer Referential Integrity",

        status=status,

        severity="CRITICAL",

        expected=0,

        actual=invalid_customers,

        remarks="All customers must exist in dim_customer."

    )

    logger.info(
        "Customer Referential Integrity validation completed."
    )

    # Product Referential Integrity Validation
    invalid_products = (

        fact_sales_df

        .join(
            dim_product_df,
            "product_id",
            "left_anti"
        )

        .count()

    )

    status = (
        "PASS"
        if invalid_products == 0
        else "FAIL"
    )

    report.add_result(

        validation_name="Product Referential Integrity",

        status=status,

        severity="CRITICAL",

        expected=0,

        actual=invalid_products,

        remarks="All products must exist in dim_product."

    )

    logger.info(
        "Product Referential Integrity validation completed."
    )

    # Seller Referential Integrity Validation
    invalid_sellers = (

        fact_sales_df

        .join(
            dim_seller_df,
            "seller_id",
            "left_anti"
        )

        .count()

    )

    status = (
        "PASS"
        if invalid_sellers == 0
        else "FAIL"
    )

    report.add_result(

        validation_name="Seller Referential Integrity",

        status=status,

        severity="CRITICAL",

        expected=0,

        actual=invalid_sellers,

        remarks="All sellers must exist in dim_seller."

    )

    logger.info(
        "Seller Referential Integrity validation completed."
    )

    # Payment Customer Referential Integrity Validation
    invalid_payment_customers = (

        fact_payments_df

        .join(
            dim_customer_df,
            "customer_id",
            "left_anti"
        )

        .count()

    )

    status = (
        "PASS"
        if invalid_payment_customers == 0
        else "FAIL"
    )

    report.add_result(

        validation_name="Payment Customer Referential Integrity",

        status=status,

        severity="CRITICAL",

        expected=0,

        actual=invalid_payment_customers,

        remarks="All payment customers must exist in dim_customer."

    )

    logger.info(
        "Payment Customer Referential Integrity validation completed."
    )

    report_path = report.write_report()

    logger.info(
        f"Validation Report Generated : {report_path}"
    )

    critical_failures = [

        row

        for row in report.results

        if row.status == "FAIL"
           and row.severity in (
               "CRITICAL",
               "ERROR"
           )

    ]

    warning_failures = [

        row

        for row in report.results

        if row.status == "FAIL"
           and row.severity == "WARNING"

    ]

    if warning_failures:
        logger.warning(

            f"{len(warning_failures)} WARNING validation(s) detected."

        )

    if critical_failures:
        for failure in critical_failures:
            logger.error(
                f"{failure.validation_name} : {failure.actual}"
            )
        raise Exception(

            f"{len(critical_failures)} CRITICAL/ERROR validation(s) failed. Check the validation report."

        )

    logger.info(
        "Gold Validation Completed Successfully."
    )

    job.commit()

except Exception:

    logger.exception(
        "Gold Validation Failed."
    )

    raise