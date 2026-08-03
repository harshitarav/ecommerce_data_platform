import boto3

from config.config import *

# ==========================================================
# Create S3 Client
# ==========================================================

s3 = boto3.client(
    "s3",
    region_name=AWS_REGION
)

# ==========================================================
# Upload File to S3
# ==========================================================

def upload_to_s3(
        local_file,
        s3_key
):

    s3.upload_file(
        local_file,
        BUCKET_NAME,
        s3_key
    )

    print(f"Uploaded -> {s3_key}")

