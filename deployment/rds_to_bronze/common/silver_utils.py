from pyspark.sql.functions import col, trim, row_number, when, initcap, upper
from pyspark.sql.window import Window

from pyspark.sql.types import StringType

def remove_soft_deleted(df):
    """
    Removes records that are marked as soft deleted.
    """

    if "is_deleted" in df.columns:
        df = df.filter(col("is_deleted") == False)

    return df

def remove_duplicates(df, key_columns, order_column="created_at"):

    if isinstance(key_columns, str):
        key_columns = [key_columns]

    if order_column is None:

        return df.dropDuplicates(key_columns)

    window_spec = (
        Window
        .partitionBy(*key_columns)
        .orderBy(col(order_column).desc())
    )

    return (
        df.withColumn("row_num", row_number().over(window_spec))
          .filter(col("row_num") == 1)
          .drop("row_num")
    )


def trim_string_columns(df):
    """
    Trims leading and trailing spaces from all string columns.
    """

    string_columns = [
        field.name
        for field in df.schema.fields
        if isinstance(field.dataType, StringType)
    ]

    for column in string_columns:
        df = df.withColumn(column, trim(col(column)))

    return df

def handle_null_values(df):
    """
    Handles NULL values based on business rules.
    """

    if "loyalty_points" in df.columns:
        df = df.withColumn(
            "loyalty_points",
            when(col("loyalty_points").isNull(), 0)
            .otherwise(col("loyalty_points"))
        )

    return df

def standardize_values(df):
    """
    Standardizes business values.
    """

    if "customer_city" in df.columns:
        df = df.withColumn(
            "customer_city",
            initcap(col("customer_city"))
        )

    if "customer_state" in df.columns:
        df = df.withColumn(
            "customer_state",
            upper(col("customer_state"))
        )

    if "order_status" in df.columns:
        df = df.withColumn(
            "order_status",
            upper(col("order_status"))
        )

    if "payment_type" in df.columns:
        df = df.withColumn(
            "payment_type",
            upper(col("payment_type"))
        )

    if "seller_city" in df.columns:
        df = df.withColumn(
            "seller_city",
            initcap(col("seller_city"))
        )

    if "seller_state" in df.columns:
        df = df.withColumn(
            "seller_state",
            upper(col("seller_state"))
        )

    if "warehouse_city" in df.columns:
        df = df.withColumn(
            "warehouse_city",
            initcap(col("warehouse_city"))
        )

    if "warehouse_state" in df.columns:
        df = df.withColumn(
            "warehouse_state",
            upper(col("warehouse_state"))
        )

    if "inventory_status" in df.columns:
        df = df.withColumn(
            "inventory_status",
            upper(col("inventory_status"))
        )

    if "carrier_name" in df.columns:
        df = df.withColumn(
            "carrier_name",
            initcap(col("carrier_name"))
        )

    if "shipment_status" in df.columns:
        df = df.withColumn(
            "shipment_status",
            upper(col("shipment_status"))
        )

    if "geolocation_city" in df.columns:
        df = df.withColumn(
            "geolocation_city",
            initcap(col("geolocation_city"))
        )

    if "geolocation_state" in df.columns:
        df = df.withColumn(
            "geolocation_state",
            upper(col("geolocation_state"))
        )

    return df

