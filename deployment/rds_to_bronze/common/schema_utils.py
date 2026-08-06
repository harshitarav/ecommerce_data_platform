import json
import pandas as pd
from sqlalchemy import text
from config.config import *

from common.s3_utils import (
    read_json_from_s3,
    write_json_to_s3
)

SCHEMA_BUCKET = BUCKET_NAME

SCHEMA_KEY = "metadata/schema.json"

def load_saved_schema():

    schema = read_json_from_s3(
        bucket_name=SCHEMA_BUCKET,
        object_key=SCHEMA_KEY
    )

    if schema is None:
        return {}

    return schema

def save_schema(schema):

    write_json_to_s3(
        data=schema,
        bucket_name=SCHEMA_BUCKET,
        object_key=SCHEMA_KEY
    )

def get_current_schema(engine, table_name):

    query = text("""
        SELECT
            COLUMN_NAME,
            DATA_TYPE
        FROM INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_SCHEMA = :database
          AND TABLE_NAME = :table
        ORDER BY ORDINAL_POSITION
    """)

    schema = pd.read_sql(
        query,
        engine,
        params={
            "database": "ecommerce",
            "table": table_name
        }
    )

    current_schema = {}

    for _, row in schema.iterrows():

        current_schema[row["COLUMN_NAME"]] = row["DATA_TYPE"]

    return current_schema

def compare_schema(saved_schema, current_schema):

    added_columns = {}
    removed_columns = {}
    datatype_changes = {}

    # Added columns
    for column, datatype in current_schema.items():
        if column not in saved_schema:
            added_columns[column] = datatype

    # Removed columns
    for column, datatype in saved_schema.items():
        if column not in current_schema:
            removed_columns[column] = datatype

    # Datatype changes
    for column in saved_schema.keys() & current_schema.keys():
        if saved_schema[column] != current_schema[column]:
            datatype_changes[column] = {
                "old": saved_schema[column],
                "new": current_schema[column]
            }

    return added_columns, removed_columns, datatype_changes

def detect_schema_changes(engine, table_name):

    saved_schemas = load_saved_schema()

    saved_schema = saved_schemas.get(table_name, {})

    current_schema = get_current_schema(engine, table_name)

    added, removed, changed = compare_schema(
        saved_schema,
        current_schema
    )

    if added or removed or changed:

        print(f"\nSchema change detected for table: {table_name}")

        if added:
            print(f"Added Columns: {added}")

        if removed:
            print(f"Removed Columns: {removed}")

        if changed:
            print(f"Datatype Changes: {changed}")

        schema_changed = True

    else:

        print(f"No schema changes detected for table: {table_name}")

        schema_changed = False

    saved_schemas[table_name] = current_schema

    save_schema(saved_schemas)

    return schema_changed