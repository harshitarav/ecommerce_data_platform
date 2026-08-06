import pandas as pd
from sqlalchemy import create_engine, text

from config.config import *


# ==========================================================
# Create Database Connection
# ==========================================================

def create_connection():

    engine = create_engine(
        f"mysql+pymysql://{USERNAME}:{PASSWORD}@{HOST}:{PORT}/{DATABASE}"
    )

    return engine


# ==========================================================
# Get All Tables
# ==========================================================

def get_all_tables(engine):

    query = "SHOW TABLES;"

    tables = pd.read_sql(query, engine)

    return tables.iloc[:, 0].tolist()


# ==========================================================
# Read Full Table
# ==========================================================

def read_full_table(engine, table_name):

    query = f"""
    SELECT *
    FROM {table_name};
    """

    return pd.read_sql(query, engine)


# ==========================================================
# Read Incremental Data
# ==========================================================

def read_incremental_data(
        engine,
        table_name,
        incremental_column,
        watermark
):

    print(f"Table = {table_name}")
    print(f"Incremental column = {incremental_column}")
    print(f"Watermark = {watermark}")

    if watermark is None:

        query = text(f"""
            SELECT *
            FROM {table_name}
        """)

        df = pd.read_sql(query, engine)

    else:

        query = text(f"""
            SELECT *
            FROM {table_name}
            WHERE {incremental_column} > :watermark
        """)

        df = pd.read_sql(
            query,
            engine,
            params={"watermark": watermark}
        )

    print(f"Rows returned = {len(df)}")

    return df

# ==========================================================
# Latest Watermark
# ==========================================================

def get_latest_watermark(
        engine,
        table_name,
        incremental_column
):

    query = text(f"""
        SELECT MAX({incremental_column})
        FROM {table_name}
    """)

    with engine.connect() as conn:

        return conn.execute(query).scalar()


MYSQL_TO_PANDAS = {
    "bigint": "Int64",
    "int": "Int64",
    "smallint": "Int64",
    "tinyint": "Int64",      # We'll keep tinyint as integer (0/1)
    "float": "float64",
    "double": "float64",
    "decimal": "float64",
    "varchar": "string",
    "char": "string",
    "text": "string",
    "datetime": "datetime64[ns]",
    "timestamp": "datetime64[ns]",
    "date": "datetime64[ns]"
}


def enforce_dataframe_schema(engine, table_name, df):

    query = text("""
        SELECT COLUMN_NAME, DATA_TYPE
        FROM INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_SCHEMA = :database
          AND TABLE_NAME = :table
        ORDER BY ORDINAL_POSITION
    """)

    schema = pd.read_sql(
        query,
        engine,
        params={
            "database": DATABASE,
            "table": table_name
        }
    )

    for _, row in schema.iterrows():

        column = row["COLUMN_NAME"]
        mysql_type = row["DATA_TYPE"].lower()

        if column not in df.columns:
            continue

        pandas_type = MYSQL_TO_PANDAS.get(mysql_type)

        if pandas_type is None:
            continue

        try:
            df[column] = df[column].astype(pandas_type)
        except Exception:
            print(f"Warning: Unable to cast {column} to {pandas_type}")

    return df