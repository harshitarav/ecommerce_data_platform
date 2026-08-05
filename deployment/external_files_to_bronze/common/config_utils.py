import json
import boto3


def load_validation_config(config_path):

    s3 = boto3.client("s3")

    if not config_path.startswith("s3://"):
        raise ValueError("CONFIG_PATH must be an S3 URI.")

    path = config_path.replace("s3://", "", 1)

    bucket, key = path.split("/", 1)

    response = s3.get_object(
        Bucket=bucket,
        Key=key
    )

    return json.loads(
        response["Body"].read().decode("utf-8")
    )