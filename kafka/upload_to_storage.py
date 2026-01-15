import os
import traceback
from typing import Optional

import boto3
from botocore.exceptions import NoCredentialsError


def _env(name: str, default: Optional[str] = None) -> Optional[str]:
    value = os.getenv(name)
    return value if value not in (None, "") else default


def get_minio_config():
    """Load MinIO config from env vars with sensible defaults.

    Env:
      - MINIO_ENDPOINT (e.g. http://minio.minio.svc.cluster.local:9000)
      - MINIO_ACCESS_KEY
      - MINIO_SECRET_KEY
    """
    return {
        "endpoint": _env("MINIO_ENDPOINT", "http://minio.minio.svc.cluster.local:9000"),
        "access_key": _env("MINIO_ACCESS_KEY", "minioadmin"),
        "secret_key": _env("MINIO_SECRET_KEY", "minioadmin"),
    }

def get_s3_client(
    endpoint_url: Optional[str] = None,
    access_key: Optional[str] = None,
    secret_key: Optional[str] = None,
):
    """Create a boto3 S3 client for MinIO."""
    try:
        cfg = get_minio_config()
        s3 = boto3.client(
            "s3",
            endpoint_url=endpoint_url or cfg["endpoint"],
            aws_access_key_id=access_key or cfg["access_key"],
            aws_secret_access_key=secret_key or cfg["secret_key"],
        )
        return s3
    except NoCredentialsError:
        print("Credentials not available")
    except Exception as e:
        print(f"Exception occurred while creating S3 client!\n{e}")
        traceback.print_exc()

def create_minio_bucket(bucket_name: str, s3=None):
    """Create a bucket if it doesn't exist."""
    try:
        s3 = s3 or get_s3_client()
        if not s3:
            raise RuntimeError("Failed to create S3 client")

        # Prefer head_bucket (cheap) over list_buckets.
        try:
            s3.head_bucket(Bucket=bucket_name)
            return
        except Exception:
            pass

        s3.create_bucket(Bucket=bucket_name)
        print(f"Bucket {bucket_name} created successfully")
    except NoCredentialsError:
        print("Credentials not available")

# Check if an object exists in a bucket
def check_object_exists(source_bucket_name, object_name):
    # Check if an object exists in MinIO
    try:
        s3 = get_s3_client()
        response = s3.head_object(Bucket=source_bucket_name, Key=object_name)
        print(f"Object {object_name} exists in {source_bucket_name}")
    except NoCredentialsError:
        print("Credentials not available")
    except s3.exceptions.NoSuchKey:
        print(f"Object {object_name} does not exist in {source_bucket_name}")

# Check if an object from one bucket to another
def copy_object(
        source_bucket_name, 
        source_object_name, 
        destination_bucket_name,
        destination_object_name):
    # Copy an object in MinIO
    try:
        # Ensure the destination bucket exists
        create_minio_bucket(destination_bucket_name)

        s3 = get_s3_client()
        s3.copy_object(
            Bucket=destination_bucket_name,
            CopySource={'Bucket': source_bucket_name, 'Key': source_object_name},
            Key=destination_object_name
        )
        print(f"Object {source_bucket_name}/{source_object_name} copied to {destination_bucket_name}/{destination_object_name}")
    except NoCredentialsError:
        print("Credentials not available")

# Delete an object from bucket
def delete_object(bucket_name, object_name):
    # Delete an object from MinIO bucket
    try:
        s3 = get_s3_client()
        s3.delete_object(Bucket=bucket_name, Key=object_name)
        print(f"Object {object_name} deleted from {bucket_name}")
    except NoCredentialsError:
        print("Credentials not available")

# Upload data from local to S3 bucket
def upload_to_s3(file_path, bucket_name, object_name):
    # Upload a file to MinIO
    try:
        s3 = get_s3_client()
        # Create bucket if not exists
        create_minio_bucket(bucket_name, s3=s3)
        # Upload the file to s3 bucket
        s3.upload_file(file_path, bucket_name, object_name)
        print(f"File uploaded successfully to {bucket_name}/{object_name}")
    except FileNotFoundError:
        print("The file was not found")
    except NoCredentialsError:
        print("Credentials not available")

# Download data from S3 bucket to local
def download_from_s3(source_bucket_name, object_name, local_file_path):
    # Download a file from MinIO
    try:
        s3 = get_s3_client()
        s3.download_file(source_bucket_name, object_name, local_file_path)
        print(f"File downloaded successfully to local [{local_file_path}]")
    except NoCredentialsError:
        print("Credentials not available")

if __name__ == "__main__":
    # Small connectivity smoke-check (optional)
    bucket = _env("MINIO_BUCKET", "hungluu-test-bucket")
    s3 = get_s3_client()
    if s3:
        create_minio_bucket(bucket, s3=s3)
        print("MinIO client OK")
