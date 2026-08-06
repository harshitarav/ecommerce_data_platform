import json
import os
import boto3
from botocore.exceptions import ClientError
from config.config import *

s3 = boto3.client("s3")

SCHEMA_BUCKET = BUCKET_NAME
WATERMARK_KEY = "metadata/watermark.json"

# ==========================================================
# Project Root
# ==========================================================

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

# ==========================================================
# Metadata Files
# ==========================================================

PROCESSED_TABLES_FILE = os.path.join(
    BASE_DIR,
    "logs",
    "processed_tables.json"
)

WATERMARK_FILE = os.path.join(
    BASE_DIR,
    "logs",
    "watermark.json"
)

TABLE_CONFIG_FILE = os.path.join(
    BASE_DIR,
    "config",
    "table_config.json"
)

# ==========================================================
# Load Processed Tables
# ==========================================================

def load_processed_tables():

    if not os.path.exists(PROCESSED_TABLES_FILE):
        return []

    with open(PROCESSED_TABLES_FILE, "r") as file:

        try:
            return json.load(file)

        except json.JSONDecodeError:
            return []

# ==========================================================
# Save Processed Tables
# ==========================================================

def save_processed_tables(processed_tables):

    with open(PROCESSED_TABLES_FILE, "w") as file:

        json.dump(
            processed_tables,
            file,
            indent=4
        )

# ==========================================================
# Load Watermarks
# ==========================================================

def load_watermarks():

    try:

        response = s3.get_object(
            Bucket=BUCKET_NAME,
            Key=WATERMARK_KEY
        )

        return json.loads(
            response["Body"].read().decode("utf-8")
        )

    except ClientError as e:

        if e.response["Error"]["Code"] == "NoSuchKey":
            return {}

        raise

# ==========================================================
# Save Watermarks
# ==========================================================

def save_watermarks(watermarks):

    s3.put_object(
        Bucket=BUCKET_NAME,
        Key=WATERMARK_KEY,
        Body=json.dumps(
            watermarks,
            indent=4,
            default=str
        ),
        ContentType="application/json"
    )

    print(
        f"Watermark saved -> s3://{BUCKET_NAME}/{WATERMARK_KEY}"
    )

# ==========================================================
# Is New Table?
# ==========================================================

def is_new_table(table_name, processed_tables):

    return table_name not in processed_tables

# ==========================================================
# Get Watermark
# ==========================================================

def get_watermark(table_name, watermarks):

    return watermarks.get(table_name)

# ==========================================================
# Update Watermark
# ==========================================================

def update_watermark(
        table_name,
        latest_watermark,
        watermarks
):

    watermarks[table_name] = str(latest_watermark)

    return watermarks

# ==========================================================
# Load Table Configuration
# ==========================================================

def load_table_config():

    with open(TABLE_CONFIG_FILE, "r") as file:

        table_config = json.load(file)

    return table_config