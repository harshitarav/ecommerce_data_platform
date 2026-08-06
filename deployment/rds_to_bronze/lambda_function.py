from datetime import datetime
import os
import traceback
import boto3

from common.db_utils import (
    create_connection,
    get_all_tables,
    read_full_table,
    read_incremental_data,
    get_latest_watermark, enforce_dataframe_schema
)
from common.metadata_utils import (
    load_processed_tables,
    save_processed_tables,
    load_watermarks,
    save_watermarks,
    is_new_table,
    get_watermark,
    update_watermark,
    load_table_config
)
from common.s3_utils import upload_to_s3
from common.schema_utils import detect_schema_changes

s3 = boto3.client("s3")


def run_etl():
    # ==========================================================
    # Create Database Connection
    # ==========================================================

    engine = create_connection()

    # ==========================================================
    # Load Metadata
    # ==========================================================

    processed_tables = load_processed_tables()

    watermarks = load_watermarks()

    table_config = load_table_config()

    # ==========================================================
    # Discover Tables
    # ==========================================================

    tables = get_all_tables(engine)

    print("=" * 70)
    print("Starting Bronze ETL")
    print("=" * 70)

    for table in tables:

        print("\n" + "=" * 70)
        print(f"Processing : {table}")

        if table not in table_config:

            print("No configuration found.")
            continue

        incremental_column = table_config[table]["incremental_column"]

        schema_changed = detect_schema_changes(
            engine,
            table
        )

        if schema_changed:
            print("Schema metadata updated.")

        if is_new_table(table, processed_tables):

            print("Initial Load")

            load_type = "FULL"

            df = read_full_table(
                engine,
                table
            )

        else:

            print("Incremental Load")

            load_type = "INCREMENTAL"

            watermark = get_watermark(
                table,
                watermarks
            )

            df = read_incremental_data(
                engine,
                table,
                incremental_column,
                watermark
            )

            if df.empty:
                print("No New Records")
                continue

        df = enforce_dataframe_schema(
            engine,
            table,
            df
        )
        print(f"Load Type : {load_type}")
        print(f"Rows : {len(df)}")

        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

        output_folder = "/tmp/generated_data"

        os.makedirs(
            output_folder,
            exist_ok=True
        )

        output_path = (
            f"{output_folder}/"
            f"{table}_{timestamp}.parquet"
        )


        df.to_parquet(
            output_path,
            engine="pyarrow",
            index=False,
            coerce_timestamps="us",
            allow_truncated_timestamps=True
        )

        print("Parquet created.")

        upload_to_s3(
            local_file=output_path,
            s3_key=f"bronze/rds/{table}/{table}_{timestamp}.parquet"
        )

        print("Uploaded Successfully")

        os.remove(output_path)

        print("Temporary local parquet deleted.")

        # ==========================================================
        # Update Watermark
        # ==========================================================

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

        if load_type == "FULL" and table not in processed_tables:
            processed_tables.append(table)
            save_processed_tables(processed_tables)
            print("Processed tables updated.")


# Lambda Handler
# def lambda_handler(event, context):
#
#     try:
#         run_etl()
#
#         return {
#             "statusCode": 200,
#             "body": "RDS to Bronze ETL completed successfully."
#         }
#
#     except Exception as e:
#         traceback.print_exc()
#         return {
#             "statusCode": 500,
#             "body": str(e)
#         }

def lambda_handler(event, context):

    print("Testing S3")

    response = s3.list_buckets()

    print(response)

    return {
        "statusCode": 200
    }