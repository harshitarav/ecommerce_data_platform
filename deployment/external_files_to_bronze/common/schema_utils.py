import json
import boto3
from botocore.exceptions import ClientError
from config.config import *


SCHEMA_BUCKET = BUCKET_NAME
SCHEMA_KEY = "metadata/schema.json"

s3 = boto3.client("s3")


def load_schema():

    try:

        response = s3.get_object(
            Bucket=BUCKET_NAME,
            Key=SCHEMA_KEY
        )

        return json.loads(
            response["Body"].read().decode("utf-8")
        )

    except ClientError as e:

        if e.response["Error"]["Code"] == "NoSuchKey":
            return {}

        raise


def save_schema(schema):

    s3.put_object(
        Bucket=BUCKET_NAME,
        Key=SCHEMA_KEY,
        Body=json.dumps(schema, indent=4),
        ContentType="application/json"
    )


def detect_schema_changes(dataset_name, df):

    saved_schema = load_schema()

    current_schema = {
        col: str(dtype)
        for col, dtype in df.dtypes.items()
    }

    old_schema = saved_schema.get(dataset_name, {})

    added = {
        k: v
        for k, v in current_schema.items()
        if k not in old_schema
    }

    removed = {
        k: v
        for k, v in old_schema.items()
        if k not in current_schema
    }

    changed = {}

    for col in current_schema:

        if (
            col in old_schema
            and current_schema[col] != old_schema[col]
        ):

            changed[col] = {
                "old": old_schema[col],
                "new": current_schema[col]
            }

    if added or removed or changed:

        print(f"\nSchema change detected : {dataset_name}")

        if added:
            print(f"Added : {added}")

        if removed:
            print(f"Removed : {removed}")

        if changed:
            print(f"Changed : {changed}")

    else:

        print(f"No schema changes : {dataset_name}")

    saved_schema[dataset_name] = current_schema

    save_schema(saved_schema)