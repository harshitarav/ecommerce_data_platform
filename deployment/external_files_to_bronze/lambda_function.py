import os
import hashlib
import boto3
import pandas as pd
import tempfile
import traceback
import logging
from botocore.exceptions import ClientError
from datetime import datetime
from common.schema_utils import detect_schema_changes

timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

def run_etl():

    # ==========================================================
    # Project Paths
    # ==========================================================

    BUCKET_NAME = "e-commerce-de-project"

    RAW_PREFIX = "raw-external/"

    LOG_PREFIX = "logs/processed_files.txt"

    TEMP_FOLDER = tempfile.gettempdir()

    # ==========================================================
    # Create Required Folders
    # ==========================================================

    os.makedirs(TEMP_FOLDER, exist_ok=True)

    # ==========================================================
    # Create S3 Client
    # ==========================================================

    s3 = boto3.client("s3")

    # ==========================================================
    # Calculate SHA256 Hash
    # ==========================================================

    def calculate_file_hash(file_path):

        sha256 = hashlib.sha256()

        with open(file_path, "rb") as file:

            while chunk := file.read(8192):

                sha256.update(chunk)

        return sha256.hexdigest()

    # ==========================================================
    # Read Metadata Log
    # ==========================================================

    processed_files = {}

    local_log = os.path.join(
        TEMP_FOLDER,
        "processed_files.txt"
    )

    try:

        s3.download_file(
            BUCKET_NAME,
            LOG_PREFIX,
            local_log
        )

        with open(local_log, "r") as file:

            for line in file:

                line = line.strip()

                if line:
                    file_name, file_hash = line.split("|")

                    processed_files[file_name] = file_hash

    except ClientError as e:
        error_code = e.response["Error"]["Code"]

        if error_code in ["404", "NoSuchKey"]:
            logger.info("Metadata file not found. Initial load.")

        else:
            logger.exception("Failed to load metadata.")
            raise

    # ==========================================================
    # Discover External CSV Files
    # ==========================================================

    response = s3.list_objects_v2(
        Bucket=BUCKET_NAME,
        Prefix=RAW_PREFIX
    )

    csv_files = [
        obj["Key"]
        for obj in response.get("Contents", [])
        if obj["Key"].endswith(".csv")
    ]

    logger.info("Starting External Files -> Bronze ETL")

    # ==========================================================
    # Process Files
    # ==========================================================

    for file_name in csv_files:

        local_csv = os.path.join(
            TEMP_FOLDER,
            os.path.basename(file_name)
        )

        s3.download_file(
            BUCKET_NAME,
            file_name,
            local_csv
        )

        current_hash = calculate_file_hash(local_csv)

        # ------------------------------------------------------
        # Skip already processed file
        # ------------------------------------------------------

        if (
            file_name in processed_files
            and processed_files[file_name] == current_hash
        ):

            logger.info(f"Skipping '{file_name}' (No Changes Detected)")
            continue

        output_file = None

        try:

            logger.info(f"Processing : {file_name}")

            dataset_name = os.path.splitext(os.path.basename(file_name))[0]

            # --------------------------------------------------
            # Read CSV
            # --------------------------------------------------

            df = pd.read_csv(local_csv)
            detect_schema_changes(
                dataset_name,
                df
            )

            logger.info(f"Rows Read : {len(df):,}")

            # --------------------------------------------------
            # Temporary Parquet File
            # --------------------------------------------------

            output_file = os.path.join(
                TEMP_FOLDER,
                f"{dataset_name}.parquet"
            )

            # --------------------------------------------------
            # Convert CSV -> Parquet
            # --------------------------------------------------

            df.to_parquet(
                output_file,
                engine="pyarrow",
                index=False
            )

            logger.info(f"{dataset_name} converted to Parquet.")

            # --------------------------------------------------
            # Upload to S3 Bronze
            # --------------------------------------------------

            s3.upload_file(
                output_file,
                BUCKET_NAME,
                f"bronze/files/{dataset_name}/{dataset_name}_{timestamp}.parquet"
            )

            logger.info(f"{dataset_name} uploaded successfully to S3 Bronze.")

            # Update metadata
            processed_files[file_name] = current_hash

        except Exception:

            logger.exception("Error processing external file.")

        finally:

            if output_file and os.path.exists(output_file):
                os.remove(output_file)

            if os.path.exists(local_csv):
                os.remove(local_csv)

    # ==========================================================
    # Save Metadata
    # ==========================================================

    local_log = os.path.join(
        TEMP_FOLDER,
        "processed_files.txt"
    )

    with open(local_log, "w") as file:

        for file_name, file_hash in processed_files.items():
            file.write(f"{file_name}|{file_hash}\n")

    s3.upload_file(
        local_log,
        BUCKET_NAME,
        LOG_PREFIX
    )

    os.remove(local_log)

    logger.info("External Files Bronze ETL Completed Successfully")

# Lambda Handler
def lambda_handler(event, context):

    try:
        run_etl()

        return {
            "statusCode": 200,
            "body": "External Files to Bronze ETL completed successfully."
        }

    except Exception as e:
        logger.exception("Error processing external file.")
        return {
            "statusCode": 500,
            "body": str(e)
        }