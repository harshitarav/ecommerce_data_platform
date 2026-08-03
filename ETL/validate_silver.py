import sys

from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions


from utils.logger import ETLLogger
from utils.validation_utils import (
    check_duplicates,
    check_nulls,
    check_column_length,
    check_allowed_values,
    check_numeric_range,
    check_required_when_status,
    check_timestamp_order,
    check_numeric_greater_than
)
from utils.report_utils import ValidationReport

# ---------------------------------------------------
# Initialize Spark and Glue
# ---------------------------------------------------

args = getResolvedOptions(sys.argv, ["JOB_NAME"])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

job = Job(glueContext)
job.init(args["JOB_NAME"], args)

report = ValidationReport(
    spark=spark,
    pipeline_name="silver",
    job_name=args["JOB_NAME"],
    bucket_name="e-commerce-de-project"
)

def process_customers():
    logger = ETLLogger(
        job_name=args["JOB_NAME"],
        layer="silver_validation",
        table_name="customers"
    )

    try:

        logger.info(
            event="JOB_START",
            message="Silver Validation Started"
        )

        # ---------------------------------------------------
        # Read Silver Customers
        # ---------------------------------------------------

        silver_dynamic_frame = glueContext.create_dynamic_frame.from_catalog(
            database="silver_db",
            table_name="customers"
        )

        customers_df = silver_dynamic_frame.toDF()

        logger.info(
            event="READ_SILVER",
            message="Customers table loaded successfully"
        )

        duplicate_count = check_duplicates(
            customers_df,
            "customer_id"
        )

        status = "PASS" if duplicate_count == 0 else "FAIL"

        report.add_result(
            validation_name="Customer Duplicate Check",
            status=status,
            severity="CRITICAL",
            expected=0,
            actual=duplicate_count,
            remarks="customer_id should be unique"
        )

        logger.info(
            event="CHECK_DUPLICATES",
            message="Duplicate validation completed",
            duplicate_count=duplicate_count
        )

        mandatory_columns = [
            "customer_id",
            "customer_unique_id",
            "customer_city",
            "customer_state"
        ]

        customer_id_invalid_length = check_column_length(
            customers_df,
            "customer_id",
            32
        )

        status = "PASS" if customer_id_invalid_length == 0 else "FAIL"

        report.add_result(
            validation_name="Customer ID Length Check",
            status=status,
            expected=32,
            severity="ERROR",
            actual=customer_id_invalid_length,
            remarks="customer_id should be exactly 32 characters"
        )

        logger.info(
            event="CHECK_COLUMN_LENGTH",
            message="Customer ID length validation completed",
            column="customer_id",
            expected_length=32,
            invalid_count=customer_id_invalid_length
        )

        #Allowed values validation
        valid_states = [
            "AC", "AL", "AP", "AM", "BA", "CE", "DF",
            "ES", "GO", "MA", "MT", "MS", "MG",
            "PA", "PB", "PR", "PE", "PI", "RJ",
            "RN", "RS", "RO", "RR", "SC", "SP",
            "SE", "TO"
        ]

        invalid_state_count = check_allowed_values(
            customers_df,
            "customer_state",
            valid_states
        )

        status = "PASS" if invalid_state_count == 0 else "FAIL"

        report.add_result(
            validation_name="Customer State Validation",
            status=status,
            severity="ERROR",
            expected="Valid Brazilian state code",
            actual=invalid_state_count,
            remarks="customer_state must contain a valid state code"
        )

        logger.info(
            event="CHECK_ALLOWED_VALUES",
            message="Customer State validation completed",
            column="customer_state",
            invalid_count=invalid_state_count
        )

        #Numeric range validation

        invalid_loyalty_points = check_numeric_range(
            customers_df,
            "loyalty_points",
            minimum=0
        )

        status = "PASS" if invalid_loyalty_points == 0 else "FAIL"

        report.add_result(
            validation_name="Loyalty Points Validation",
            status=status,
            severity="ERROR",
            expected=">= 0",
            actual=invalid_loyalty_points,
            remarks="loyalty_points must be greater than or equal to 0"
        )

        logger.info(
            event="CHECK_NUMERIC_RANGE",
            message="Loyalty Points validation completed",
            column="loyalty_points",
            minimum=0,
            invalid_count=invalid_loyalty_points
        )

        total_null_count = 0

        for column in mandatory_columns:
            null_count = check_nulls(
                customers_df,
                column
            )

            total_null_count += null_count

            status = "PASS" if null_count == 0 else "FAIL"

            report.add_result(
                validation_name=f"{column} NULL Check",
                status=status,
                severity="CRITICAL",
                expected=0,
                actual=null_count,
                remarks=f"{column} should not contain NULL values"
            )

            logger.info(
                event="CHECK_NULLS",
                message="NULL validation completed",
                column=column,
                null_count=null_count
            )

        logger.info(
            event="VALIDATION_COMPLETED",
            message="Customer validations completed"
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
            message="Silver Validation Job Finished"
        )

def process_orders():

    logger = ETLLogger(
        job_name=args["JOB_NAME"],
        layer="silver_validation",
        table_name="orders"
    )

    try:

        logger.info(
            event="JOB_START",
            message="Orders Silver Validation Started"
        )

        silver_dynamic_frame = glueContext.create_dynamic_frame.from_catalog(
            database="silver_db",
            table_name="orders"
        )

        orders_df = silver_dynamic_frame.toDF()

        logger.info(
            event="READ_SILVER",
            message="Orders table loaded successfully"
        )

        # ---------------------------------------------------
        # Duplicate Validation
        # ---------------------------------------------------

        duplicate_count = check_duplicates(
            orders_df,
            "order_id"
        )

        status = "PASS" if duplicate_count == 0 else "FAIL"

        report.add_result(
            validation_name="Order Duplicate Check",
            status=status,
            severity="CRITICAL",
            expected=0,
            actual=duplicate_count,
            remarks="order_id should be unique"
        )

        logger.info(
            event="CHECK_DUPLICATES",
            message="Duplicate validation completed",
            duplicate_count=duplicate_count
        )

        # ---------------------------------------------------
        # Mandatory NULL Validation
        # ---------------------------------------------------

        mandatory_columns = [
            "order_id",
            "customer_id",
            "order_status",
            "order_purchase_timestamp",
            "order_estimated_delivery_date"
        ]

        for column in mandatory_columns:
            null_count = check_nulls(
                orders_df,
                column
            )

            status = "PASS" if null_count == 0 else "FAIL"

            report.add_result(
                validation_name=f"{column} NULL Check",
                status=status,
                severity="CRITICAL",
                expected=0,
                actual=null_count,
                remarks=f"{column} should not contain NULL values"
            )

            logger.info(
                event="CHECK_NULLS",
                message="NULL validation completed",
                column=column,
                null_count=null_count
            )

        # ---------------------------------------------------
        # order_id Length Validation
        # ---------------------------------------------------

        invalid_order_id_length = check_column_length(
            orders_df,
            "order_id",
            32
        )

        status = "PASS" if invalid_order_id_length == 0 else "FAIL"

        report.add_result(
            validation_name="Order ID Length Check",
            status=status,
            severity="ERROR",
            expected=32,
            actual=invalid_order_id_length,
            remarks="order_id should be exactly 32 characters"
        )

        logger.info(
            event="CHECK_COLUMN_LENGTH",
            message="Order ID length validation completed",
            column="order_id",
            expected_length=32,
            invalid_count=invalid_order_id_length
        )

        # ---------------------------------------------------
        # Allowed Status Validation
        # ---------------------------------------------------

        valid_status = [
            "DELIVERED",
            "SHIPPED",
            "CANCELED",
            "UNAVAILABLE",
            "INVOICED",
            "PROCESSING",
            "CREATED",
            "APPROVED"
        ]

        invalid_status_count = check_allowed_values(
            orders_df,
            "order_status",
            valid_status
        )

        status = "PASS" if invalid_status_count == 0 else "FAIL"

        report.add_result(
            validation_name="Order Status Validation",
            status=status,
            severity="ERROR",
            expected="Valid Order Status",
            actual=invalid_status_count,
            remarks="order_status must contain a valid status"
        )

        logger.info(
            event="CHECK_ALLOWED_VALUES",
            message="Order Status validation completed",
            column="order_status",
            invalid_count=invalid_status_count
        )

        # ---------------------------------------------------
        # Delivered orders must have customer delivery date
        # ---------------------------------------------------

        missing_delivery_date = check_required_when_status(
            orders_df,
            status_column="order_status",
            required_column="order_delivered_customer_date",
            status_values=["DELIVERED"]
        )

        status = "PASS" if missing_delivery_date == 0 else "FAIL"

        report.add_result(
            validation_name="Delivered Customer Date Validation",
            status=status,
            severity="WARNING",
            expected="Required for DELIVERED orders",
            actual=missing_delivery_date,
            remarks="DELIVERED orders must have order_delivered_customer_date"
        )

        logger.info(
            event="CHECK_REQUIRED_WHEN_STATUS",
            message="Delivered orders customer delivery date validation completed",
            column="order_delivered_customer_date",
            invalid_count=missing_delivery_date
        )

        missing_carrier_date = check_required_when_status(
            orders_df,
            status_column="order_status",
            required_column="order_delivered_carrier_date",
            status_values=["DELIVERED"]
        )

        status = "PASS" if missing_carrier_date == 0 else "FAIL"

        report.add_result(
            validation_name="Delivered Carrier Date Validation",
            status=status,
            severity="WARNING",
            expected="Required for DELIVERED orders",
            actual=missing_carrier_date,
            remarks="DELIVERED orders must have order_delivered_carrier_date"
        )

        logger.info(
            event="CHECK_REQUIRED_WHEN_STATUS",
            message="Delivered orders carrier delivery date validation completed",
            column="order_delivered_carrier_date",
            invalid_count=missing_carrier_date
        )

        missing_approval_date = check_required_when_status(
            orders_df,
            status_column="order_status",
            required_column="order_approved_at",
            status_values=[
                "APPROVED",
                "PROCESSING",
                "INVOICED",
                "SHIPPED",
                "DELIVERED"
            ]
        )

        status = "PASS" if missing_approval_date == 0 else "FAIL"

        report.add_result(
            validation_name="Approved Timestamp Validation",
            status=status,
            severity="WARNING",
            expected="Required for APPROVED, PROCESSING, INVOICED, SHIPPED and DELIVERED orders",
            actual=missing_approval_date,
            remarks="order_approved_at must exist for applicable order statuses"
        )

        logger.info(
            event="CHECK_REQUIRED_WHEN_STATUS",
            message="Approved timestamp validation completed",
            column="order_approved_at",
            invalid_count=missing_approval_date
        )

        # ---------------------------------------------------
        # Purchase Timestamp <= Approval Timestamp
        # ---------------------------------------------------

        invalid_purchase_approval = check_timestamp_order(
            orders_df,
            earlier_column="order_purchase_timestamp",
            later_column="order_approved_at"
        )

        status = "PASS" if invalid_purchase_approval == 0 else "FAIL"

        report.add_result(
            validation_name="Purchase to Approval Timestamp Validation",
            status=status,
            severity="WARNING",
            expected="order_purchase_timestamp <= order_approved_at",
            actual=invalid_purchase_approval,
            remarks="Purchase timestamp must be earlier than or equal to approval timestamp"
        )

        logger.info(
            event="CHECK_TIMESTAMP_ORDER",
            message="Purchase to Approval timestamp validation completed",
            earlier_column="order_purchase_timestamp",
            later_column="order_approved_at",
            invalid_count=invalid_purchase_approval
        )

        # ---------------------------------------------------
        # Approval Timestamp <= Carrier Timestamp
        # ---------------------------------------------------

        invalid_approval_carrier = check_timestamp_order(
            orders_df,
            earlier_column="order_approved_at",
            later_column="order_delivered_carrier_date",
            status_column="order_status",
            status_values=[
                "SHIPPED",
                "DELIVERED"
            ]
        )

        status = "PASS" if invalid_approval_carrier == 0 else "FAIL"

        report.add_result(
            validation_name="Approval to Carrier Timestamp Validation",
            status=status,
            severity="WARNING",
            expected="order_approved_at <= order_delivered_carrier_date",
            actual=invalid_approval_carrier,
            remarks="Approval timestamp must be earlier than or equal to carrier delivery timestamp"
        )

        logger.info(
            event="CHECK_TIMESTAMP_ORDER",
            message="Approval to Carrier timestamp validation completed",
            earlier_column="order_approved_at",
            later_column="order_delivered_carrier_date",
            invalid_count=invalid_approval_carrier
        )

        # ---------------------------------------------------
        # Carrier Timestamp <= Customer Delivery Timestamp
        # ---------------------------------------------------

        invalid_carrier_delivery = check_timestamp_order(
            orders_df,
            earlier_column="order_delivered_carrier_date",
            later_column="order_delivered_customer_date",
            status_column="order_status",
            status_values=[
                "DELIVERED"
            ]
        )

        status = "PASS" if invalid_carrier_delivery == 0 else "FAIL"

        report.add_result(
            validation_name="Carrier to Customer Delivery Timestamp Validation",
            status=status,
            severity="WARNING",
            expected="order_delivered_carrier_date <= order_delivered_customer_date",
            actual=invalid_carrier_delivery,
            remarks="Carrier delivery timestamp must be earlier than or equal to customer delivery timestamp"
        )

        logger.info(
            event="CHECK_TIMESTAMP_ORDER",
            message="Carrier to Customer Delivery timestamp validation completed",
            earlier_column="order_delivered_carrier_date",
            later_column="order_delivered_customer_date",
            invalid_count=invalid_carrier_delivery
        )

        # ---------------------------------------------------
        # Purchase Timestamp <= Estimated Delivery
        # ---------------------------------------------------

        invalid_purchase_estimated = check_timestamp_order(
            orders_df,
            earlier_column="order_purchase_timestamp",
            later_column="order_estimated_delivery_date"
        )

        status = "PASS" if invalid_purchase_estimated == 0 else "FAIL"

        report.add_result(
            validation_name="Purchase to Estimated Delivery Timestamp Validation",
            status=status,
            severity="WARNING",
            expected="order_purchase_timestamp <= order_estimated_delivery_date",
            actual=invalid_purchase_estimated,
            remarks="Purchase timestamp must be earlier than or equal to estimated delivery date"
        )

        logger.info(
            event="CHECK_TIMESTAMP_ORDER",
            message="Purchase to Estimated Delivery timestamp validation completed",
            earlier_column="order_purchase_timestamp",
            later_column="order_estimated_delivery_date",
            invalid_count=invalid_purchase_estimated
        )

        logger.info(
            event="VALIDATION_COMPLETED",
            message="Orders validations completed"
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
            message="Orders Silver Validation Job Finished"
        )

def process_order_items():

    logger = ETLLogger(
        job_name=args["JOB_NAME"],
        layer="silver_validation",
        table_name="order_items"
    )

    try:

        logger.info(
            event="JOB_START",
            message="Order Items Silver Validation Started"
        )

        silver_dynamic_frame = glueContext.create_dynamic_frame.from_catalog(
            database="silver_db",
            table_name="order_items"
        )

        order_items_df = silver_dynamic_frame.toDF()

        logger.info(
            event="READ_SILVER",
            message="Order Items table loaded successfully"
        )

        # -----------------------------------------
        # Duplicate Validation
        # -----------------------------------------

        duplicate_count = check_duplicates(
            order_items_df,
            ["order_id", "order_item_id"]
        )

        status = "PASS" if duplicate_count == 0 else "FAIL"

        report.add_result(
            validation_name="Order Items Duplicate Check",
            status=status,
            severity="CRITICAL",
            expected=0,
            actual=duplicate_count,
            remarks="Combination of order_id and order_item_id should be unique"
        )

        logger.info(
            event="CHECK_DUPLICATES",
            message="Duplicate validation completed",
            duplicate_count=duplicate_count
        )

        # -----------------------------------------
        # Mandatory NULL Validation
        # -----------------------------------------

        mandatory_columns = [
            "order_id",
            "order_item_id",
            "product_id",
            "seller_id",
            "shipping_limit_date",
            "price",
            "freight_value"
        ]

        for column in mandatory_columns:
            null_count = check_nulls(
                order_items_df,
                column
            )

            status = "PASS" if null_count == 0 else "FAIL"

            report.add_result(
                validation_name=f"{column} NULL Check",
                status=status,
                severity="CRITICAL",
                expected=0,
                actual=null_count,
                remarks=f"{column} should not contain NULL values"
            )

            logger.info(
                event="CHECK_NULLS",
                message="NULL validation completed",
                column=column,
                null_count=null_count
            )

        # -----------------------------------------
        # Length Validation
        # -----------------------------------------

        invalid_order_id = check_column_length(
            order_items_df,
            "order_id",
            32
        )

        status = "PASS" if invalid_order_id == 0 else "FAIL"

        report.add_result(
            validation_name="Order Item Order ID Length Check",
            status=status,
            severity="ERROR",
            expected=32,
            actual=invalid_order_id,
            remarks="order_id should be exactly 32 characters"
        )

        logger.info(
            event="CHECK_COLUMN_LENGTH",
            message="Order ID validation completed",
            column="order_id",
            expected_length=32,
            invalid_count=invalid_order_id
        )

        invalid_product_id = check_column_length(
            order_items_df,
            "product_id",
            32
        )

        status = "PASS" if invalid_product_id == 0 else "FAIL"

        report.add_result(
            validation_name="Order Item Product ID Length Check",
            status=status,
            severity="ERROR",
            expected=32,
            actual=invalid_product_id,
            remarks="product_id should be exactly 32 characters"
        )

        logger.info(
            event="CHECK_COLUMN_LENGTH",
            message="Product ID validation completed",
            column="product_id",
            expected_length=32,
            invalid_count=invalid_product_id
        )

        invalid_seller_id = check_column_length(
            order_items_df,
            "seller_id",
            32
        )

        status = "PASS" if invalid_seller_id == 0 else "FAIL"

        report.add_result(
            validation_name="Order Item Seller ID Length Check",
            status=status,
            severity="ERROR",
            expected=32,
            actual=invalid_seller_id,
            remarks="seller_id should be exactly 32 characters"
        )

        logger.info(
            event="CHECK_COLUMN_LENGTH",
            message="Seller ID validation completed",
            column="seller_id",
            expected_length=32,
            invalid_count=invalid_seller_id
        )

        # -----------------------------------------
        # Numeric Validations
        # -----------------------------------------

        invalid_price = check_numeric_range(
            order_items_df,
            "price",
            minimum=0
        )

        status = "PASS" if invalid_price == 0 else "FAIL"

        report.add_result(
            validation_name="Price Validation",
            status=status,
            severity="ERROR",
            expected=">= 0",
            actual=invalid_price,
            remarks="price must be greater than or equal to 0"
        )

        logger.info(
            event="CHECK_NUMERIC_RANGE",
            message="Price validation completed",
            column="price",
            minimum=0,
            invalid_count=invalid_price
        )

        invalid_freight = check_numeric_range(
            order_items_df,
            "freight_value",
            minimum=0
        )

        status = "PASS" if invalid_freight == 0 else "FAIL"

        report.add_result(
            validation_name="Freight Value Validation",
            status=status,
            severity="ERROR",
            expected=">= 0",
            actual=invalid_freight,
            remarks="freight_value must be greater than or equal to 0"
        )

        logger.info(
            event="CHECK_NUMERIC_RANGE",
            message="Freight validation completed",
            column="freight_value",
            minimum=0,
            invalid_count=invalid_freight
        )

        invalid_item_number = check_numeric_range(
            order_items_df,
            "order_item_id",
            minimum=1
        )

        status = "PASS" if invalid_item_number == 0 else "FAIL"

        report.add_result(
            validation_name="Order Item ID Validation",
            status=status,
            severity="ERROR",
            expected=">= 1",
            actual=invalid_item_number,
            remarks="order_item_id must be greater than or equal to 1"
        )

        logger.info(
            event="CHECK_NUMERIC_RANGE",
            message="Order Item ID validation completed",
            column="order_item_id",
            minimum=1,
            invalid_count=invalid_item_number
        )

        logger.info(
            event="VALIDATION_COMPLETED",
            message="Order Items validations completed"
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
            message="Order Items Silver Validation Job Finished"
        )

def process_products():

    logger = ETLLogger(
        job_name=args["JOB_NAME"],
        layer="silver_validation",
        table_name="products"
    )

    try:

        logger.info(
            event="JOB_START",
            message="Products Silver Validation Started"
        )

        # ---------------------------------------------------
        # Read Silver Products
        # ---------------------------------------------------

        silver_dynamic_frame = glueContext.create_dynamic_frame.from_catalog(
            database="silver_db",
            table_name="products"
        )

        products_df = silver_dynamic_frame.toDF()

        logger.info(
            event="READ_SILVER",
            message="Products table loaded successfully"
        )

        # ---------------------------------------------------
        # Duplicate Validation
        # ---------------------------------------------------

        duplicate_count = check_duplicates(
            products_df,
            "product_id"
        )

        status = "PASS" if duplicate_count == 0 else "FAIL"

        report.add_result(
            validation_name="Product Duplicate Check",
            status=status,
            severity="CRITICAL",
            expected=0,
            actual=duplicate_count,
            remarks="product_id should be unique"
        )

        logger.info(
            event="CHECK_DUPLICATES",
            message="Duplicate validation completed",
            duplicate_count=duplicate_count
        )

        # ---------------------------------------------------
        # Mandatory NULL Validation
        # ---------------------------------------------------

        mandatory_columns = [
            "product_id"
        ]

        for column in mandatory_columns:
            null_count = check_nulls(
                products_df,
                column
            )

            status = "PASS" if null_count == 0 else "FAIL"

            report.add_result(
                validation_name=f"{column} NULL Check",
                status=status,
                severity="CRITICAL",
                expected=0,
                actual=null_count,
                remarks=f"{column} should not contain NULL values"
            )

            logger.info(
                event="CHECK_NULLS",
                message="NULL validation completed",
                column=column,
                null_count=null_count
            )

        # ---------------------------------------------------
        # Product Category NULL Validation
        # ---------------------------------------------------

        null_category_count = check_nulls(
            products_df,
            "product_category_name"
        )

        status = "PASS" if null_category_count == 0 else "FAIL"

        report.add_result(
            validation_name="Product Category NULL Check",
            status=status,
            severity="WARNING",
            expected=0,
            actual=null_category_count,
            remarks="product_category_name should not be NULL"
        )

        logger.info(
            event="CHECK_NULLS",
            message="Product Category validation completed",
            column="product_category_name",
            null_count=null_category_count
        )

        # ---------------------------------------------------
        # Product Name Length Validation
        # ---------------------------------------------------

        invalid_product_name_length = check_numeric_greater_than(
            products_df,
            "product_name_lenght",
            0
        )

        status = "PASS" if invalid_product_name_length == 0 else "FAIL"

        report.add_result(
            validation_name="Product Name Length Validation",
            status=status,
            severity="ERROR",
            expected="> 0",
            actual=invalid_product_name_length,
            remarks="product_name_lenght must be greater than 0"
        )

        logger.info(
            event="CHECK_NUMERIC_RANGE",
            message="Product Name Length validation completed",
            column="product_name_lenght",
            rule="> 0",
            invalid_count=invalid_product_name_length
        )

        # ---------------------------------------------------
        # Product Photos Quantity Validation
        # ---------------------------------------------------

        invalid_photo_count = check_numeric_range(
            products_df,
            "product_photos_qty",
            minimum=1
        )

        status = "PASS" if invalid_photo_count == 0 else "FAIL"

        report.add_result(
            validation_name="Product Photos Quantity Validation",
            status=status,
            severity="ERROR",
            expected=">= 1",
            actual=invalid_photo_count,
            remarks="product_photos_qty must be greater than or equal to 1"
        )

        logger.info(
            event="CHECK_NUMERIC_RANGE",
            message="Product Photos Quantity validation completed",
            column="product_photos_qty",
            minimum=1,
            invalid_count=invalid_photo_count
        )

        # ---------------------------------------------------
        # Product ID Length Validation
        # ---------------------------------------------------

        invalid_product_id = check_column_length(
            products_df,
            "product_id",
            32
        )

        status = "PASS" if invalid_product_id == 0 else "FAIL"

        report.add_result(
            validation_name="Product ID Length Check",
            status=status,
            severity="ERROR",
            expected=32,
            actual=invalid_product_id,
            remarks="product_id should be exactly 32 characters"
        )

        logger.info(
            event="CHECK_COLUMN_LENGTH",
            message="Product ID validation completed",
            column="product_id",
            expected_length=32,
            invalid_count=invalid_product_id
        )

        # ---------------------------------------------------
        # Numeric Range Validation
        # ---------------------------------------------------

        numeric_columns = [
            "product_description_lenght",
            "product_weight_g",
            "product_length_cm",
            "product_height_cm",
            "product_width_cm"
        ]

        for column in numeric_columns:
            invalid_count = check_numeric_range(
                products_df,
                column,
                minimum=0
            )

            status = "PASS" if invalid_count == 0 else "FAIL"

            report.add_result(
                validation_name=f"{column} Validation",
                status=status,
                severity="ERROR",
                expected=">= 0",
                actual=invalid_count,
                remarks=f"{column} must be greater than or equal to 0"
            )

            logger.info(
                event="CHECK_NUMERIC_RANGE",
                message=f"{column} validation completed",
                column=column,
                minimum=0,
                invalid_count=invalid_count
            )

        logger.info(
            event="VALIDATION_COMPLETED",
            message="Products validations completed"
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
            message="Products Silver Validation Job Finished"
        )

def process_payments():

    logger = ETLLogger(
        job_name=args["JOB_NAME"],
        layer="silver_validation",
        table_name="payments"
    )

    try:

        logger.info(
            event="JOB_START",
            message="Payments Silver Validation Started"
        )

        # ---------------------------------------------------
        # Read Silver Payments
        # ---------------------------------------------------

        silver_dynamic_frame = glueContext.create_dynamic_frame.from_catalog(
            database="silver_db",
            table_name="payments"
        )

        payments_df = silver_dynamic_frame.toDF()

        logger.info(
            event="READ_SILVER",
            message="Payments table loaded successfully"
        )

        # ---------------------------------------------------
        # Duplicate Validation
        # ---------------------------------------------------

        duplicate_count = check_duplicates(
            payments_df,
            ["order_id", "payment_sequential"]
        )

        status = "PASS" if duplicate_count == 0 else "FAIL"

        report.add_result(
            validation_name="Payment Duplicate Check",
            status=status,
            severity="CRITICAL",
            expected=0,
            actual=duplicate_count,
            remarks="Combination of order_id and payment_sequential should be unique"
        )

        logger.info(
            event="CHECK_DUPLICATES",
            message="Duplicate validation completed",
            duplicate_count=duplicate_count
        )

        # ---------------------------------------------------
        # Mandatory NULL Validation
        # ---------------------------------------------------

        mandatory_columns = [
            "order_id",
            "payment_sequential",
            "payment_type",
            "payment_installments",
            "payment_value"
        ]

        for column in mandatory_columns:
            null_count = check_nulls(
                payments_df,
                column
            )

            status = "PASS" if null_count == 0 else "FAIL"

            report.add_result(
                validation_name=f"{column} NULL Check",
                status=status,
                severity="CRITICAL",
                expected=0,
                actual=null_count,
                remarks=f"{column} should not contain NULL values"
            )

            logger.info(
                event="CHECK_NULLS",
                message="NULL validation completed",
                column=column,
                null_count=null_count
            )

        # ---------------------------------------------------
        # Order ID Length Validation
        # ---------------------------------------------------

        invalid_order_id = check_column_length(
            payments_df,
            "order_id",
            32
        )

        status = "PASS" if invalid_order_id == 0 else "FAIL"

        report.add_result(
            validation_name="Payment Order ID Length Check",
            status=status,
            severity="ERROR",
            expected=32,
            actual=invalid_order_id,
            remarks="order_id should be exactly 32 characters"
        )

        logger.info(
            event="CHECK_COLUMN_LENGTH",
            message="Order ID validation completed",
            column="order_id",
            expected_length=32,
            invalid_count=invalid_order_id
        )

        # ---------------------------------------------------
        # Allowed Payment Types
        # ---------------------------------------------------

        valid_payment_types = [
            "CREDIT_CARD",
            "BOLETO",
            "VOUCHER",
            "DEBIT_CARD",
            "NOT_DEFINED"
        ]

        invalid_payment_type = check_allowed_values(
            payments_df,
            "payment_type",
            valid_payment_types
        )

        status = "PASS" if invalid_payment_type == 0 else "FAIL"

        report.add_result(
            validation_name="Payment Type Validation",
            status=status,
            severity="ERROR",
            expected="Valid Payment Type",
            actual=invalid_payment_type,
            remarks="payment_type must contain a valid payment type"
        )

        logger.info(
            event="CHECK_ALLOWED_VALUES",
            message="Payment Type validation completed",
            column="payment_type",
            invalid_count=invalid_payment_type
        )

        # ---------------------------------------------------
        # Payment Sequential Validation
        # ---------------------------------------------------

        invalid_payment_sequence = check_numeric_range(
            payments_df,
            "payment_sequential",
            minimum=1
        )

        status = "PASS" if invalid_payment_sequence == 0 else "FAIL"

        report.add_result(
            validation_name="Payment Sequential Validation",
            status=status,
            severity="ERROR",
            expected=">= 1",
            actual=invalid_payment_sequence,
            remarks="payment_sequential must be greater than or equal to 1"
        )

        logger.info(
            event="CHECK_NUMERIC_RANGE",
            message="Payment Sequential validation completed",
            column="payment_sequential",
            minimum=1,
            invalid_count=invalid_payment_sequence
        )

        # ---------------------------------------------------
        # Payment Installments Validation
        # ---------------------------------------------------

        invalid_installments = check_numeric_range(
            payments_df,
            "payment_installments",
            minimum=0
        )

        status = "PASS" if invalid_installments == 0 else "FAIL"

        report.add_result(
            validation_name="Payment Installments Validation",
            status=status,
            severity="ERROR",
            expected=">= 0",
            actual=invalid_installments,
            remarks="payment_installments must be greater than or equal to 0"
        )

        logger.info(
            event="CHECK_NUMERIC_RANGE",
            message="Payment Installments validation completed",
            column="payment_installments",
            minimum=0,
            invalid_count=invalid_installments
        )

        # ---------------------------------------------------
        # Payment Value Validation
        # ---------------------------------------------------

        invalid_payment_value = check_numeric_range(
            payments_df,
            "payment_value",
            minimum=0
        )

        status = "PASS" if invalid_payment_value == 0 else "FAIL"

        report.add_result(
            validation_name="Payment Value Validation",
            status=status,
            severity="ERROR",
            expected=">= 0",
            actual=invalid_payment_value,
            remarks="payment_value must be greater than or equal to 0"
        )

        logger.info(
            event="CHECK_NUMERIC_RANGE",
            message="Payment Value validation completed",
            column="payment_value",
            minimum=0,
            invalid_count=invalid_payment_value
        )

        logger.info(
            event="VALIDATION_COMPLETED",
            message="Payments validations completed"
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
            message="Payments Silver Validation Job Finished"
        )

def process_sellers():

    logger = ETLLogger(
        job_name=args["JOB_NAME"],
        layer="silver_validation",
        table_name="sellers"
    )

    try:

        logger.info(
            event="JOB_START",
            message="Sellers Silver Validation Started"
        )

        # ---------------------------------------------------
        # Read Silver Sellers
        # ---------------------------------------------------

        silver_dynamic_frame = glueContext.create_dynamic_frame.from_catalog(
            database="silver_db",
            table_name="sellers"
        )

        sellers_df = silver_dynamic_frame.toDF()

        logger.info(
            event="READ_SILVER",
            message="Sellers table loaded successfully"
        )

        # ---------------------------------------------------
        # Duplicate Validation
        # ---------------------------------------------------

        duplicate_count = check_duplicates(
            sellers_df,
            "seller_id"
        )

        status = "PASS" if duplicate_count == 0 else "FAIL"

        report.add_result(
            validation_name="Seller Duplicate Check",
            status=status,
            severity="CRITICAL",
            expected=0,
            actual=duplicate_count,
            remarks="seller_id should be unique"
        )

        logger.info(
            event="CHECK_DUPLICATES",
            message="Duplicate validation completed",
            duplicate_count=duplicate_count
        )

        # ---------------------------------------------------
        # Mandatory NULL Validation
        # ---------------------------------------------------

        mandatory_columns = [
            "seller_id",
            "seller_zip_code_prefix",
            "seller_city",
            "seller_state"
        ]

        for column in mandatory_columns:
            null_count = check_nulls(
                sellers_df,
                column
            )

            status = "PASS" if null_count == 0 else "FAIL"

            report.add_result(
                validation_name=f"{column} NULL Check",
                status=status,
                severity="CRITICAL",
                expected=0,
                actual=null_count,
                remarks=f"{column} should not contain NULL values"
            )

            logger.info(
                event="CHECK_NULLS",
                message="NULL validation completed",
                column=column,
                null_count=null_count
            )

        # ---------------------------------------------------
        # Seller ID Length Validation
        # ---------------------------------------------------

        invalid_seller_id = check_column_length(
            sellers_df,
            "seller_id",
            32
        )

        status = "PASS" if invalid_seller_id == 0 else "FAIL"

        report.add_result(
            validation_name="Seller ID Length Check",
            status=status,
            severity="ERROR",
            expected=32,
            actual=invalid_seller_id,
            remarks="seller_id should be exactly 32 characters"
        )

        logger.info(
            event="CHECK_COLUMN_LENGTH",
            message="Seller ID validation completed",
            column="seller_id",
            expected_length=32,
            invalid_count=invalid_seller_id
        )

        # ---------------------------------------------------
        # Allowed Seller State Validation
        # ---------------------------------------------------

        valid_states = [
            "AC", "AL", "AP", "AM", "BA", "CE", "DF",
            "ES", "GO", "MA", "MT", "MS", "MG",
            "PA", "PB", "PR", "PE", "PI", "RJ",
            "RN", "RS", "RO", "RR", "SC", "SP",
            "SE", "TO"
        ]

        invalid_state_count = check_allowed_values(
            sellers_df,
            "seller_state",
            valid_states
        )

        status = "PASS" if invalid_state_count == 0 else "FAIL"

        report.add_result(
            validation_name="Seller State Validation",
            status=status,
            severity="ERROR",
            expected="Valid Brazilian State",
            actual=invalid_state_count,
            remarks="seller_state must contain a valid Brazilian state code"
        )

        logger.info(
            event="CHECK_ALLOWED_VALUES",
            message="Seller State validation completed",
            column="seller_state",
            invalid_count=invalid_state_count
        )

        # ---------------------------------------------------
        # ZIP Prefix Validation
        # ---------------------------------------------------

        invalid_zip = check_numeric_range(
            sellers_df,
            "seller_zip_code_prefix",
            minimum=0
        )

        status = "PASS" if invalid_zip == 0 else "FAIL"

        report.add_result(
            validation_name="Seller ZIP Prefix Validation",
            status=status,
            severity="ERROR",
            expected=">= 0",
            actual=invalid_zip,
            remarks="seller_zip_code_prefix must be greater than or equal to 0"
        )

        logger.info(
            event="CHECK_NUMERIC_RANGE",
            message="Seller ZIP Prefix validation completed",
            column="seller_zip_code_prefix",
            minimum=0,
            invalid_count=invalid_zip
        )

        logger.info(
            event="VALIDATION_COMPLETED",
            message="Sellers validations completed"
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
            message="Sellers Silver Validation Job Finished"
        )

def process_reviews():

    logger = ETLLogger(
        job_name=args["JOB_NAME"],
        layer="silver_validation",
        table_name="reviews"
    )

    try:

        logger.info(
            event="JOB_START",
            message="Reviews Silver Validation started"
        )

        # ---------------------------------------------------
        # Read Silver Reviews
        # ---------------------------------------------------


        silver_dynamic_frame = glueContext.create_dynamic_frame.from_catalog(
            database="silver_db",
            table_name="reviews"
        )

        reviews_df = silver_dynamic_frame.toDF()

        logger.info(
            event="READ_SILVER",
            message="Reading reviews table from Silver"
        )

        rows_read = reviews_df.count()

        logger.info(
            event="ROWS_READ",
            message="Initial row count",
            rows_read=rows_read
        )

        # =====================================================
        # Duplicate Validation
        # =====================================================

        duplicate_count = check_duplicates(
            reviews_df,
            ["review_id", "order_id"]
        )

        status = "PASS" if duplicate_count == 0 else "FAIL"

        report.add_result(
            validation_name="Review Duplicate Check",
            status=status,
            severity="CRITICAL",
            expected=0,
            actual=duplicate_count,
            remarks="Combination of review_id and order_id should be unique"
        )

        logger.info(
            event="CHECK_DUPLICATES",
            message="Duplicate validation completed",
            duplicate_count=duplicate_count
        )

        # =====================================================
        # NULL Validation
        # =====================================================

        mandatory_columns = [
            "review_id",
            "order_id",
            "review_score",
            "review_creation_date",
            "review_answer_timestamp"
        ]

        for column in mandatory_columns:
            null_count = check_nulls(
                reviews_df,
                column
            )

            status = "PASS" if null_count == 0 else "FAIL"

            report.add_result(
                validation_name=f"{column} NULL Check",
                status=status,
                severity="CRITICAL",
                expected=0,
                actual=null_count,
                remarks=f"{column} should not contain NULL values"
            )

            logger.info(
                event="CHECK_NULLS",
                message="NULL validation completed",
                column=column,
                null_count=null_count
            )

        # =====================================================
        # review_id Length Validation
        # =====================================================

        review_id_length_count = check_column_length(
            reviews_df,
            "review_id",
            32
        )

        status = "PASS" if review_id_length_count == 0 else "FAIL"

        report.add_result(
            validation_name="Review ID Length Check",
            status=status,
            severity="ERROR",
            expected=32,
            actual=review_id_length_count,
            remarks="review_id should be exactly 32 characters"
        )

        logger.info(
            event="CHECK_REVIEW_ID_LENGTH",
            message="review_id length validation completed",
            invalid_count=review_id_length_count
        )

        # =====================================================
        # order_id Length Validation
        # =====================================================

        order_id_length_count = check_column_length(
            reviews_df,
            "order_id",
            32
        )

        status = "PASS" if order_id_length_count == 0 else "FAIL"

        report.add_result(
            validation_name="Review Order ID Length Check",
            status=status,
            severity="ERROR",
            expected=32,
            actual=order_id_length_count,
            remarks="order_id should be exactly 32 characters"
        )

        logger.info(
            event="CHECK_ORDER_ID_LENGTH",
            message="order_id length validation completed",
            invalid_count=order_id_length_count
        )

        # =====================================================
        # Review Score Validation
        # =====================================================

        review_score_count = check_numeric_range(
            reviews_df,
            "review_score",
            minimum=1,
            maximum=5
        )

        status = "PASS" if review_score_count == 0 else "FAIL"

        report.add_result(
            validation_name="Review Score Validation",
            status=status,
            severity="ERROR",
            expected="1 to 5",
            actual=review_score_count,
            remarks="review_score must be between 1 and 5"
        )

        logger.info(
            event="CHECK_REVIEW_SCORE_RANGE",
            message="Review score validation completed",
            invalid_count=review_score_count
        )

        # =====================================================
        # Timestamp Order Validation
        # =====================================================

        timestamp_count = check_timestamp_order(
            reviews_df,
            "review_creation_date",
            "review_answer_timestamp"
        )

        status = "PASS" if timestamp_count == 0 else "FAIL"

        report.add_result(
            validation_name="Review Timestamp Validation",
            status=status,
            severity="ERROR",
            expected="review_creation_date <= review_answer_timestamp",
            actual=timestamp_count,
            remarks="Review creation date must be earlier than or equal to review answer timestamp"
        )

        logger.info(
            event="CHECK_TIMESTAMP_ORDER",
            message="Timestamp order validation completed",
            invalid_count=timestamp_count
        )

        logger.info(
            event="VALIDATION_COMPLETED",
            message="Reviews validations completed"
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
            message="Reviews Silver Validation finished"
        )

def process_inventory():

    logger = ETLLogger(
        job_name=args["JOB_NAME"],
        layer="silver_validation",
        table_name="inventory"
    )

    try:

        logger.info(
            event="JOB_START",
            message="Inventory Silver Validation Started"
        )

        # ---------------------------------------------------
        # Read Silver Inventory
        # ---------------------------------------------------

        silver_dynamic_frame = glueContext.create_dynamic_frame.from_catalog(
            database="silver_db",
            table_name="inventory"
        )

        inventory_df = silver_dynamic_frame.toDF()

        logger.info(
            event="READ_SILVER",
            message="Inventory table loaded successfully"
        )

        # ---------------------------------------------------
        # Duplicate Validation
        # ---------------------------------------------------

        duplicate_count = check_duplicates(
            inventory_df,
            "inventory_id"
        )

        status = "PASS" if duplicate_count == 0 else "FAIL"

        report.add_result(
            validation_name="Inventory Duplicate Check",
            status=status,
            severity="CRITICAL",
            expected=0,
            actual=duplicate_count,
            remarks="inventory_id should be unique"
        )

        logger.info(
            event="CHECK_DUPLICATES",
            message="Duplicate validation completed",
            duplicate_count=duplicate_count
        )

        # ---------------------------------------------------
        # Mandatory NULL Validation
        # ---------------------------------------------------

        mandatory_columns = [
            "inventory_id",
            "warehouse_id",
            "warehouse_city",
            "warehouse_state",
            "product_id",
            "available_stock",
            "reserved_stock",
            "safety_stock",
            "reorder_level",
            "inventory_status"
        ]

        for column in mandatory_columns:
            null_count = check_nulls(
                inventory_df,
                column
            )

            status = "PASS" if null_count == 0 else "FAIL"

            report.add_result(
                validation_name=f"{column} NULL Check",
                status=status,
                severity="CRITICAL",
                expected=0,
                actual=null_count,
                remarks=f"{column} should not contain NULL values"
            )

            logger.info(
                event="CHECK_NULLS",
                message="NULL validation completed",
                column=column,
                null_count=null_count
            )
        null_count = check_nulls(
            inventory_df,
            "last_updated"
        )

        status = "PASS" if null_count == 0 else "FAIL"

        report.add_result(
            validation_name="last_updated NULL Check",
            status=status,
            severity="WARNING",
            expected=0,
            actual=null_count,
            remarks="last_updated is optional in source data"
        )


        # ---------------------------------------------------
        # Inventory ID Length Validation
        # ---------------------------------------------------

        invalid_inventory_id = check_column_length(
            inventory_df,
            "inventory_id",
            9
        )

        status = "PASS" if invalid_inventory_id == 0 else "FAIL"

        report.add_result(
            validation_name="Inventory ID Length Check",
            status=status,
            severity="ERROR",
            expected=9,
            actual=invalid_inventory_id,
            remarks="inventory_id should be exactly 9 characters"
        )

        logger.info(
            event="CHECK_COLUMN_LENGTH",
            message="Inventory ID validation completed",
            column="inventory_id",
            expected_length=9,
            invalid_count=invalid_inventory_id
        )

        # ---------------------------------------------------
        # Product ID Length Validation
        # ---------------------------------------------------

        invalid_product_id = check_column_length(
            inventory_df,
            "product_id",
            32
        )

        status = "PASS" if invalid_product_id == 0 else "FAIL"

        report.add_result(
            validation_name="Inventory Product ID Length Check",
            status=status,
            severity="ERROR",
            expected=32,
            actual=invalid_product_id,
            remarks="product_id should be exactly 32 characters"
        )

        logger.info(
            event="CHECK_COLUMN_LENGTH",
            message="Product ID validation completed",
            column="product_id",
            expected_length=32,
            invalid_count=invalid_product_id
        )

        # ---------------------------------------------------
        # Warehouse State Validation
        # ---------------------------------------------------

        valid_states = [
            "AC", "AL", "AP", "AM", "BA", "CE", "DF",
            "ES", "GO", "MA", "MT", "MS", "MG",
            "PA", "PB", "PR", "PE", "PI", "RJ",
            "RN", "RS", "RO", "RR", "SC", "SP",
            "SE", "TO"
        ]

        invalid_state_count = check_allowed_values(
            inventory_df,
            "warehouse_state",
            valid_states
        )

        status = "PASS" if invalid_state_count == 0 else "FAIL"

        report.add_result(
            validation_name="Warehouse State Validation",
            status=status,
            severity="ERROR",
            expected="Valid Brazilian State",
            actual=invalid_state_count,
            remarks="warehouse_state must contain a valid Brazilian state code"
        )

        logger.info(
            event="CHECK_ALLOWED_VALUES",
            message="Warehouse State validation completed",
            column="warehouse_state",
            invalid_count=invalid_state_count
        )

        # ---------------------------------------------------
        # Available Stock Validation
        # ---------------------------------------------------

        invalid_available_stock = check_numeric_range(
            inventory_df,
            "available_stock",
            minimum=0
        )

        status = "PASS" if invalid_available_stock == 0 else "FAIL"

        report.add_result(
            validation_name="Available Stock Validation",
            status=status,
            severity="ERROR",
            expected=">= 0",
            actual=invalid_available_stock,
            remarks="available_stock must be greater than or equal to 0"
        )

        logger.info(
            event="CHECK_NUMERIC_RANGE",
            message="Available Stock validation completed",
            column="available_stock",
            minimum=0,
            invalid_count=invalid_available_stock
        )

        # ---------------------------------------------------
        # Reserved Stock Validation
        # ---------------------------------------------------

        invalid_reserved_stock = check_numeric_range(
            inventory_df,
            "reserved_stock",
            minimum=0
        )

        status = "PASS" if invalid_reserved_stock == 0 else "FAIL"

        report.add_result(
            validation_name="Reserved Stock Validation",
            status=status,
            severity="ERROR",
            expected=">= 0",
            actual=invalid_reserved_stock,
            remarks="reserved_stock must be greater than or equal to 0"
        )

        logger.info(
            event="CHECK_NUMERIC_RANGE",
            message="Reserved Stock validation completed",
            column="reserved_stock",
            minimum=0,
            invalid_count=invalid_reserved_stock
        )

        # ---------------------------------------------------
        # Safety Stock Validation
        # ---------------------------------------------------

        invalid_safety_stock = check_numeric_range(
            inventory_df,
            "safety_stock",
            minimum=0
        )

        status = "PASS" if invalid_safety_stock == 0 else "FAIL"

        report.add_result(
            validation_name="Safety Stock Validation",
            status=status,
            severity="ERROR",
            expected=">= 0",
            actual=invalid_safety_stock,
            remarks="safety_stock must be greater than or equal to 0"
        )

        logger.info(
            event="CHECK_NUMERIC_RANGE",
            message="Safety Stock validation completed",
            column="safety_stock",
            minimum=0,
            invalid_count=invalid_safety_stock
        )

        # ---------------------------------------------------
        # Reorder Level Validation
        # ---------------------------------------------------

        invalid_reorder_level = check_numeric_range(
            inventory_df,
            "reorder_level",
            minimum=0
        )

        status = "PASS" if invalid_reorder_level == 0 else "FAIL"

        report.add_result(
            validation_name="Reorder Level Validation",
            status=status,
            severity="ERROR",
            expected=">= 0",
            actual=invalid_reorder_level,
            remarks="reorder_level must be greater than or equal to 0"
        )

        logger.info(
            event="CHECK_NUMERIC_RANGE",
            message="Reorder Level validation completed",
            column="reorder_level",
            minimum=0,
            invalid_count=invalid_reorder_level
        )

        # ---------------------------------------------------
        # Inventory Status Validation
        # ---------------------------------------------------

        valid_inventory_status = [
            "DISCONTINUED",
            "IN_STOCK",
            "LOW_STOCK",
            "OUT_OF_STOCK"
        ]

        invalid_status = check_allowed_values(
            inventory_df,
            "inventory_status",
            valid_inventory_status
        )

        status = "PASS" if invalid_status == 0 else "FAIL"

        report.add_result(
            validation_name="Inventory Status Validation",
            status=status,
            severity="ERROR",
            expected="Valid Inventory Status",
            actual=invalid_status,
            remarks="inventory_status must contain a valid inventory status"
        )

        logger.info(
            event="CHECK_ALLOWED_VALUES",
            message="Inventory Status validation completed",
            column="inventory_status",
            invalid_count=invalid_status
        )

        logger.info(
            event="VALIDATION_COMPLETED",
            message="Inventory validations completed"
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
            message="Inventory Silver Validation Job Finished"
        )

def process_shipment():

    logger = ETLLogger(
        job_name=args["JOB_NAME"],
        layer="silver_validation",
        table_name="shipment"
    )

    try:

        logger.info(
            event="JOB_START",
            message="Shipment Silver Validation Started"
        )

        silver_dynamic_frame = glueContext.create_dynamic_frame.from_catalog(
            database="silver_db",
            table_name="shipment"
        )

        shipment_df = silver_dynamic_frame.toDF()

        logger.info(
            event="READ_SILVER",
            message="Shipment table loaded successfully"
        )

        # ---------------------------------------------------
        # Duplicate Validation
        # ---------------------------------------------------

        duplicate_count = check_duplicates(
            shipment_df,
            "shipment_id"
        )

        status = "PASS" if duplicate_count == 0 else "FAIL"

        report.add_result(
            validation_name="Shipment Duplicate Check",
            status=status,
            severity="CRITICAL",
            expected=0,
            actual=duplicate_count,
            remarks="shipment_id should be unique"
        )

        logger.info(
            event="CHECK_DUPLICATES",
            message="Duplicate validation completed",
            duplicate_count=duplicate_count
        )

        # ---------------------------------------------------
        # Mandatory NULL Validation
        # ---------------------------------------------------

        mandatory_columns = [
            "shipment_id",
            "tracking_number",
            "order_id",
            "order_item_id",
            "product_id",
            "seller_id",
            "warehouse_id",
            "warehouse_city",
            "warehouse_state",
            "carrier_name",
            "shipping_cost",
            "shipment_status"
        ]

        for column in mandatory_columns:
            null_count = check_nulls(
                shipment_df,
                column
            )

            status = "PASS" if null_count == 0 else "FAIL"

            report.add_result(
                validation_name=f"{column} NULL Check",
                status=status,
                severity="CRITICAL",
                expected=0,
                actual=null_count,
                remarks=f"{column} should not contain NULL values"
            )

            logger.info(
                event="CHECK_NULLS",
                message="NULL validation completed",
                column=column,
                null_count=null_count
            )

        # ---------------------------------------------------
        # shipped_timestamp Required Validation
        # ---------------------------------------------------

        missing_shipped = check_required_when_status(
            shipment_df,
            status_column="shipment_status",
            required_column="shipped_timestamp",
            status_values=[
                "SHIPPED",
                "DELIVERED"
            ]
        )

        status = "PASS" if missing_shipped == 0 else "FAIL"

        report.add_result(
            validation_name="shipped_timestamp Validation",
            status=status,
            severity="WARNING",
            expected="Required for SHIPPED and DELIVERED shipments",
            actual=missing_shipped,
            remarks="shipped_timestamp required only after shipment"
        )

        logger.info(
            event="CHECK_REQUIRED_WHEN_STATUS",
            message="shipped_timestamp validation completed",
            column="shipped_timestamp",
            invalid_count=missing_shipped
        )

        # ---------------------------------------------------
        # Shipment ID Length Validation
        # ---------------------------------------------------

        invalid_shipment_id = check_column_length(
            shipment_df,
            "shipment_id",
            12
        )

        status = "PASS" if invalid_shipment_id == 0 else "FAIL"

        report.add_result(
            validation_name="Shipment ID Length Check",
            status=status,
            severity="ERROR",
            expected=12,
            actual=invalid_shipment_id,
            remarks="shipment_id should be exactly 12 characters"
        )

        logger.info(
            event="CHECK_COLUMN_LENGTH",
            message="Shipment ID validation completed",
            column="shipment_id",
            expected_length=12,
            invalid_count=invalid_shipment_id
        )

        # ---------------------------------------------------
        # Tracking Number Length Validation
        # ---------------------------------------------------

        invalid_tracking = check_column_length(
            shipment_df,
            "tracking_number",
            17
        )

        status = "PASS" if invalid_tracking == 0 else "FAIL"

        report.add_result(
            validation_name="Tracking Number Length Check",
            status=status,
            severity="ERROR",
            expected=17,
            actual=invalid_tracking,
            remarks="tracking_number should be exactly 17 characters"
        )

        logger.info(
            event="CHECK_COLUMN_LENGTH",
            message="Tracking Number validation completed",
            column="tracking_number",
            expected_length=17,
            invalid_count=invalid_tracking
        )

        # ---------------------------------------------------
        # Product ID Length Validation
        # ---------------------------------------------------

        invalid_product = check_column_length(
            shipment_df,
            "product_id",
            32
        )

        status = "PASS" if invalid_product == 0 else "FAIL"

        report.add_result(
            validation_name="Shipment Product ID Length Check",
            status=status,
            severity="ERROR",
            expected=32,
            actual=invalid_product,
            remarks="product_id should be exactly 32 characters"
        )

        logger.info(
            event="CHECK_COLUMN_LENGTH",
            message="Product ID validation completed",
            column="product_id",
            expected_length=32,
            invalid_count=invalid_product
        )

        # ---------------------------------------------------
        # Seller ID Length Validation
        # ---------------------------------------------------

        invalid_seller = check_column_length(
            shipment_df,
            "seller_id",
            32
        )

        status = "PASS" if invalid_seller == 0 else "FAIL"

        report.add_result(
            validation_name="Shipment Seller ID Length Check",
            status=status,
            severity="ERROR",
            expected=32,
            actual=invalid_seller,
            remarks="seller_id should be exactly 32 characters"
        )

        logger.info(
            event="CHECK_COLUMN_LENGTH",
            message="Seller ID validation completed",
            column="seller_id",
            expected_length=32,
            invalid_count=invalid_seller
        )

        # ---------------------------------------------------
        # Warehouse State Validation
        # ---------------------------------------------------

        valid_states = [
            "AC", "AL", "AP", "AM", "BA", "CE", "DF",
            "ES", "GO", "MA", "MT", "MS", "MG",
            "PA", "PB", "PR", "PE", "PI", "RJ",
            "RN", "RS", "RO", "RR", "SC", "SP",
            "SE", "TO"
        ]

        invalid_state = check_allowed_values(
            shipment_df,
            "warehouse_state",
            valid_states
        )

        status = "PASS" if invalid_state == 0 else "FAIL"

        report.add_result(
            validation_name="Warehouse State Validation",
            status=status,
            severity="ERROR",
            expected="Valid Brazilian State",
            actual=invalid_state,
            remarks="warehouse_state must contain a valid Brazilian state code"
        )

        logger.info(
            event="CHECK_ALLOWED_VALUES",
            message="Warehouse State validation completed",
            column="warehouse_state",
            invalid_count=invalid_state
        )

        # ---------------------------------------------------
        # Shipment Status Validation
        # ---------------------------------------------------

        valid_status = [
            "CANCELLED",
            "DELIVERED",
            "PACKED",
            "SHIPPED"
        ]

        invalid_status = check_allowed_values(
            shipment_df,
            "shipment_status",
            valid_status
        )

        status = "PASS" if invalid_status == 0 else "FAIL"

        report.add_result(
            validation_name="Shipment Status Validation",
            status=status,
            severity="ERROR",
            expected="Valid Shipment Status",
            actual=invalid_status,
            remarks="shipment_status must contain a valid shipment status"
        )

        logger.info(
            event="CHECK_ALLOWED_VALUES",
            message="Shipment Status validation completed",
            column="shipment_status",
            invalid_count=invalid_status
        )

        # ---------------------------------------------------
        # Shipment Timestamp Validation
        # ---------------------------------------------------

        invalid_timestamp = check_timestamp_order(
            shipment_df,
            "shipped_timestamp",
            "delivered_timestamp"
        )

        status = "PASS" if invalid_timestamp == 0 else "FAIL"

        report.add_result(
            validation_name="Shipment Timestamp Validation",
            status=status,
            severity="WARNING",
            expected="shipped_timestamp <= delivered_timestamp",
            actual=invalid_timestamp,
            remarks="Shipment timestamp must be earlier than or equal to delivered timestamp"
        )

        logger.info(
            event="CHECK_TIMESTAMP_ORDER",
            message="Shipment timestamp validation completed",
            invalid_count=invalid_timestamp
        )

        # ---------------------------------------------------
        # Delivered Timestamp Required
        # ---------------------------------------------------

        missing_delivery = check_required_when_status(
            shipment_df,
            status_column="shipment_status",
            required_column="delivered_timestamp",
            status_values=["DELIVERED"]
        )

        status = "PASS" if missing_delivery == 0 else "FAIL"

        report.add_result(
            validation_name="Delivered Timestamp Validation",
            status=status,
            severity="WARNING",
            expected="Required for DELIVERED shipments",
            actual=missing_delivery,
            remarks="DELIVERED shipments must have delivered_timestamp"
        )

        logger.info(
            event="CHECK_REQUIRED_WHEN_STATUS",
            message="Delivered timestamp validation completed",
            column="delivered_timestamp",
            invalid_count=missing_delivery
        )

        # ---------------------------------------------------
        # Shipping Cost Validation
        # ---------------------------------------------------

        invalid_shipping_cost = check_numeric_range(
            shipment_df,
            "shipping_cost",
            minimum=0
        )

        status = "PASS" if invalid_shipping_cost == 0 else "FAIL"

        report.add_result(
            validation_name="Shipping Cost Validation",
            status=status,
            severity="ERROR",
            expected=">= 0",
            actual=invalid_shipping_cost,
            remarks="shipping_cost must be greater than or equal to 0"
        )

        logger.info(
            event="CHECK_NUMERIC_RANGE",
            message="Shipping Cost validation completed",
            column="shipping_cost",
            minimum=0,
            invalid_count=invalid_shipping_cost
        )

        logger.info(
            event="VALIDATION_COMPLETED",
            message="Shipment validations completed"
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
            message="Shipment Silver Validation Job Finished"
        )

def process_geolocation():

    logger = ETLLogger(
        job_name=args["JOB_NAME"],
        layer="silver_validation",
        table_name="geolocation"
    )

    try:

        logger.info(
            event="JOB_START",
            message="Geolocation Silver Validation Started"
        )

        # ---------------------------------------------------
        # Read Silver Geolocation
        # ---------------------------------------------------

        silver_dynamic_frame = glueContext.create_dynamic_frame.from_catalog(
            database="silver_db",
            table_name="geolocation"
        )

        geolocation_df = silver_dynamic_frame.toDF()

        logger.info(
            event="READ_SILVER",
            message="Geolocation table loaded successfully"
        )

        # ---------------------------------------------------
        # Duplicate Validation
        # ---------------------------------------------------

        duplicate_count = check_duplicates(
            geolocation_df,
            [
                "geolocation_zip_code_prefix",
                "geolocation_lat",
                "geolocation_lng"
            ]
        )

        status = "PASS" if duplicate_count == 0 else "FAIL"

        report.add_result(
            validation_name="Geolocation Duplicate Check",
            status=status,
            severity="CRITICAL",
            expected=0,
            actual=duplicate_count,
            remarks="ZIP code, latitude and longitude combination should be unique"
        )

        logger.info(
            event="CHECK_DUPLICATES",
            message="Duplicate validation completed",
            duplicate_count=duplicate_count
        )

        # ---------------------------------------------------
        # Mandatory NULL Validation
        # ---------------------------------------------------

        mandatory_columns = [
            "geolocation_zip_code_prefix",
            "geolocation_lat",
            "geolocation_lng",
            "geolocation_city",
            "geolocation_state"
        ]

        for column in mandatory_columns:
            null_count = check_nulls(
                geolocation_df,
                column
            )

            status = "PASS" if null_count == 0 else "FAIL"

            report.add_result(
                validation_name=f"{column} NULL Check",
                status=status,
                severity="CRITICAL",
                expected=0,
                actual=null_count,
                remarks=f"{column} should not contain NULL values"
            )

            logger.info(
                event="CHECK_NULLS",
                message="NULL validation completed",
                column=column,
                null_count=null_count
            )

        # ---------------------------------------------------
        # Allowed State Validation
        # ---------------------------------------------------

        valid_states = [
            "AC", "AL", "AP", "AM", "BA", "CE", "DF",
            "ES", "GO", "MA", "MT", "MS", "MG",
            "PA", "PB", "PR", "PE", "PI", "RJ",
            "RN", "RS", "RO", "RR", "SC", "SP",
            "SE", "TO"
        ]

        invalid_state = check_allowed_values(
            geolocation_df,
            "geolocation_state",
            valid_states
        )

        status = "PASS" if invalid_state == 0 else "FAIL"

        report.add_result(
            validation_name="Geolocation State Validation",
            status=status,
            severity="ERROR",
            expected="Valid Brazilian State",
            actual=invalid_state,
            remarks="geolocation_state must contain a valid Brazilian state code"
        )

        logger.info(
            event="CHECK_ALLOWED_VALUES",
            message="Geolocation State validation completed",
            column="geolocation_state",
            invalid_count=invalid_state
        )

        # ---------------------------------------------------
        # ZIP Prefix Validation
        # ---------------------------------------------------

        invalid_zip = check_numeric_range(
            geolocation_df,
            "geolocation_zip_code_prefix",
            minimum=0
        )

        status = "PASS" if invalid_zip == 0 else "FAIL"

        report.add_result(
            validation_name="ZIP Prefix Validation",
            status=status,
            severity="ERROR",
            expected=">= 0",
            actual=invalid_zip,
            remarks="ZIP code prefix must be greater than or equal to 0"
        )

        logger.info(
            event="CHECK_NUMERIC_RANGE",
            message="ZIP Prefix validation completed",
            column="geolocation_zip_code_prefix",
            minimum=0,
            invalid_count=invalid_zip
        )

        # ---------------------------------------------------
        # Latitude Validation
        # ---------------------------------------------------

        invalid_latitude = check_numeric_range(
            geolocation_df,
            "geolocation_lat",
            minimum=-90,
            maximum=90
        )

        status = "PASS" if invalid_latitude == 0 else "FAIL"

        report.add_result(
            validation_name="Latitude Validation",
            status=status,
            severity="ERROR",
            expected="-90 to 90",
            actual=invalid_latitude,
            remarks="Latitude must be between -90 and 90 degrees"
        )

        logger.info(
            event="CHECK_NUMERIC_RANGE",
            message="Latitude validation completed",
            column="geolocation_lat",
            minimum=-90,
            maximum=90,
            invalid_count=invalid_latitude
        )

        # ---------------------------------------------------
        # Longitude Validation
        # ---------------------------------------------------

        invalid_longitude = check_numeric_range(
            geolocation_df,
            "geolocation_lng",
            minimum=-180,
            maximum=180
        )

        status = "PASS" if invalid_longitude == 0 else "FAIL"

        report.add_result(
            validation_name="Longitude Validation",
            status=status,
            severity="ERROR",
            expected="-180 to 180",
            actual=invalid_longitude,
            remarks="Longitude must be between -180 and 180 degrees"
        )

        logger.info(
            event="CHECK_NUMERIC_RANGE",
            message="Longitude validation completed",
            column="geolocation_lng",
            minimum=-180,
            maximum=180,
            invalid_count=invalid_longitude
        )

        logger.info(
            event="VALIDATION_COMPLETED",
            message="Geolocation validations completed"
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
            message="Geolocation Silver Validation Job Finished"
        )


def main():
    logger = ETLLogger(
        job_name=args["JOB_NAME"],
        layer="silver_validation",
        table_name="pipeline"
    )

    try:

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

        report_path = report.write_report()

        if report_path:

            print(
                f"Validation Report Written : {report_path}"
            )

        critical_failures = [
            row
            for row in report.results
            if row.status == "FAIL"
               and row.severity == "CRITICAL"
        ]

        if critical_failures:

            for failure in critical_failures:
                logger.error(
                    event="VALIDATION_FAILED",
                    message=failure.validation_name,
                    severity=failure.severity,
                    actual=failure.actual
                )

            raise Exception(
                f"{len(critical_failures)} validation(s) failed."
            )

        job.commit()

    except Exception:

        report_path = report.write_report()

        if report_path:

            print(
                f"Validation Report Written : {report_path}"
            )

        raise


if __name__ == "__main__":
    main()