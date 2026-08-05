import json
import os
import pandas as pd
from sqlalchemy import text

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

SCHEMA_FILE = os.path.join(
    BASE_DIR,
    "logs",
    "schema.json"
)

def load_saved_schema():

    if not os.path.exists(SCHEMA_FILE):
        return {}

    if os.path.getsize(SCHEMA_FILE) == 0:
        return {}

    with open(SCHEMA_FILE, "r") as file:
        return json.load(file)

def save_schema(schema):

    with open(SCHEMA_FILE, "w") as file:
        json.dump(
            schema,
            file,
            indent=4
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