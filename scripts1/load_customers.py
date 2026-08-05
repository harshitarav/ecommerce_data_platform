import pandas as pd
from sqlalchemy import create_engine

# ==============================
# AWS RDS Configuration
# ==============================
HOST = "ecommerce-mysql-db.ca1w862eecuo.us-east-1.rds.amazonaws.com"
PORT = 3306
DATABASE = "ecommerce"
USERNAME = "admin"
PASSWORD = "ecommerce1234"

# ==============================
# CSV Path
# ==============================
CSV_FILE = r"C:\Users\harsh\OneDrive\Desktop\DE PREP\Ecommerce-DE-Project\datasets\olist_order_reviews_dataset.csv"


# ==============================
# Create Connection
# ==============================
engine = create_engine(
    f"mysql+pymysql://{USERNAME}:{PASSWORD}@{HOST}:{PORT}/{DATABASE}"
)

print("Reading CSV...")

df = pd.read_csv(CSV_FILE)

# Convert datetime columns
date_columns = [
    "review_creation_date",
    "review_answer_timestamp"
]

for col in date_columns:
    df[col] = pd.to_datetime(df[col], errors="coerce")

print(df.head())
print(f"Rows : {len(df)}")

print("Uploading to RDS...")

from sqlalchemy import text

with engine.connect() as conn:
    result = conn.execute(text("SELECT 1"))
    print(result.fetchone())

print("Total rows:", len(df))
print("Unique review_id:", df["review_id"].nunique())
print("Duplicate review_ids:", df["review_id"].duplicated().sum())

df.to_sql(
    "reviews",
    engine,
    if_exists="append",
    index=False,
    method="multi",
    chunksize=1000
)

print("Reviews loaded successfully!")