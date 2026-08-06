import json
import boto3
from botocore.exceptions import ClientError
print("s3_utils module imported")
from config.config import *


print("***** S3_UTILS LOADED *****")

# ==========================================================
# Create S3 Client
# ==========================================================

s3 = boto3.client(
    "s3",
    region_name=AWS_REGION
)
print("S3 client created")
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

def read_json_from_s3(
        bucket_name,
        object_key
):
    print("Entering read_json_from_s3")
    try:
        print("Calling get_object...")
        response = s3.get_object(
            Bucket=bucket_name,
            Key=object_key
        )
        print("get_object returned")

        return json.loads(
            response["Body"].read().decode("utf-8")
        )

    except ClientError as e:

        if e.response["Error"]["Code"] == "NoSuchKey":
            return None

        raise

def write_json_to_s3(
        data,
        bucket_name,
        object_key
):

    s3.put_object(
        Bucket=bucket_name,
        Key=object_key,
        Body=json.dumps(
            data,
            indent=4
        ),
        ContentType="application/json"
    )

    print(f"Schema saved -> s3://{bucket_name}/{object_key}")

