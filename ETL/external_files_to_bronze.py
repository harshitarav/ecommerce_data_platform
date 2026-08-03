import os
import hashlib
import boto3
import pandas as pd
import tempfile

# ==========================================================
# AWS Configuration
# ==========================================================

BUCKET_NAME = "e-commerce-de-project"

# ==========================================================
# Project Paths
# ==========================================================

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

DATASET_FOLDER = os.path.join(BASE_DIR, "source_data", "external")

LOG_FOLDER = os.path.join(BASE_DIR, "logs")

LOG_FILE = os.path.join(LOG_FOLDER, "processed_files.txt")

TEMP_FOLDER = tempfile.gettempdir()

# ==========================================================
# Create Required Folders
# ==========================================================

os.makedirs(LOG_FOLDER, exist_ok=True)
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

if os.path.exists(LOG_FILE):

    with open(LOG_FILE, "r") as file:

        for line in file:

            line = line.strip()

            if line:

                file_name, file_hash = line.split("|")

                processed_files[file_name] = file_hash

# ==========================================================
# Discover External CSV Files
# ==========================================================

csv_files = sorted([
    file
    for file in os.listdir(DATASET_FOLDER)
    if file.lower().endswith(".csv")
])

print("=" * 70)
print("Starting External Files -> Bronze ETL")
print("=" * 70)

# ==========================================================
# Process Files
# ==========================================================

for file_name in csv_files:

    input_file = os.path.join(DATASET_FOLDER, file_name)

    current_hash = calculate_file_hash(input_file)

    # ------------------------------------------------------
    # Skip already processed file
    # ------------------------------------------------------

    if (
        file_name in processed_files
        and processed_files[file_name] == current_hash
    ):

        print(f"Skipping '{file_name}' (No Changes Detected)")
        continue

    try:

        print("\n" + "=" * 60)
        print(f"Processing : {file_name}")

        dataset_name = os.path.splitext(file_name)[0]

        # --------------------------------------------------
        # Read CSV
        # --------------------------------------------------

        df = pd.read_csv(input_file)

        print(f"Rows Read : {len(df):,}")

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

        print(f"{dataset_name} converted to Parquet.")

        # --------------------------------------------------
        # Upload to S3 Bronze
        # --------------------------------------------------

        s3.upload_file(
            output_file,
            BUCKET_NAME,
            f"bronze/files/{dataset_name}/{dataset_name}.parquet"
        )

        print(f"{dataset_name} uploaded successfully to S3 Bronze.")

        # --------------------------------------------------
        # Delete Temporary File
        # --------------------------------------------------

        if os.path.exists(output_file):
            os.remove(output_file)

            print("Temporary parquet deleted.")

        # --------------------------------------------------
        # Update Metadata
        # --------------------------------------------------

        processed_files[file_name] = current_hash

    except Exception as e:

        print(f"Error processing '{file_name}'")
        print(e)

# ==========================================================
# Save Metadata
# ==========================================================

with open(LOG_FILE, "w") as file:

    for file_name, file_hash in processed_files.items():

        file.write(f"{file_name}|{file_hash}\n")

print("\n" + "=" * 70)
print("External Files Bronze ETL Completed Successfully")
print("=" * 70)