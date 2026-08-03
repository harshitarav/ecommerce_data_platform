import os
from datetime import datetime
import pandas as pd
import boto3
import traceback

from utils.db_utils import get_latest_watermark, read_full_table, enforce_dataframe_schema
from utils.metadata_utils import (
    load_processed_tables,
    save_processed_tables,
    load_watermarks,
    save_watermarks,
    update_watermark,
    load_table_config
)
from utils.db_utils import create_connection
from utils.schema_utils import detect_schema_changes

# =====================================
# RDS Connection
# =====================================

engine = create_connection()

# =====================================
# Load Metadata
# =====================================

processed_tables = load_processed_tables()

watermarks = load_watermarks()

table_config = load_table_config()

# =====================================
# S3 Client
# =====================================

s3 = boto3.client("s3")

BUCKET_NAME = "e-commerce-de-project"

# =====================================
# Tables
# =====================================

TABLES = [
    "customers",
    "orders",
    "products",
    "payments",
    "reviews",
    "sellers",
    "order_items"
]

# =====================================
# Process Every Table
# =====================================

for table in TABLES:

    try:

        print("=" * 60)
        print(f"Processing table : {table}")

        # -----------------------------
        # Read from RDS
        # -----------------------------

        schema_changed = detect_schema_changes(
            engine,
            table
        )

        if schema_changed:
            print("Schema metadata updated.")

        df = read_full_table(
            engine,
            table
        )


        df = enforce_dataframe_schema(
            engine,
            table,
            df
        )

        print(f"Rows : {len(df)}")

        # -----------------------------
        # Create Local Folder
        # -----------------------------


        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

        output_folder = "../generated_data"

        os.makedirs(
            output_folder,
            exist_ok=True
        )

        output_path = (
            f"{output_folder}/"
            f"{table}_{timestamp}.parquet"
        )

        # -----------------------------
        # Save as Parquet
        # -----------------------------


        df.to_parquet(
            output_path,
            engine="pyarrow",
            index=False,
            coerce_timestamps="us",
            allow_truncated_timestamps=True
        )


        print("Parquet created.")

        # -----------------------------
        # Upload to S3
        # -----------------------------

        s3.upload_file(
            output_path,
            BUCKET_NAME,
            f"bronze/rds/{table}/{table}_{timestamp}.parquet"
        )

        print(f"{table} uploaded successfully.")

        # -----------------------------
        # Delete Local Parquet
        # -----------------------------

        os.remove(output_path)

        print("Temporary local parquet deleted.")

        # =====================================
        # Update Watermark
        # =====================================

        incremental_column = table_config[table]["incremental_column"]

        latest_watermark = get_latest_watermark(
            engine,
            table,
            incremental_column
        )

        watermarks = update_watermark(
            table,
            latest_watermark,
            watermarks
        )

        save_watermarks(
            watermarks
        )

        print("Watermark updated.")

        # =====================================
        # Update Processed Tables
        # =====================================

        if table not in processed_tables:
            processed_tables.append(table)

            save_processed_tables(
                processed_tables
            )

        print("Processed tables updated.")
    except Exception as e:

        print(f"Error processing {table}")
        traceback.print_exc()

print("=" * 60)
print("RDS to Bronze Initial Load Completed.")