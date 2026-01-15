import os

from pyspark.sql import SparkSession


def _env(name: str, default: str) -> str:
    v = os.getenv(name)
    return v if v not in (None, "") else default


def build_spark(app_name: str) -> SparkSession:
    """Create SparkSession with MinIO (S3A) defaults.

    Env:
      - MINIO_ENDPOINT (http://host:9000)
      - MINIO_ACCESS_KEY / MINIO_SECRET_KEY
      - S3A_PATH_STYLE_ACCESS (true|false)
      - SPARK_S3_PACKAGES (optional)

    Note: If your Spark image does not include Hadoop AWS jars, either:
      - set SPARK_S3_PACKAGES, or
      - pass --packages to spark-submit.
    """

    endpoint = _env("MINIO_ENDPOINT", "http://minio.minio.svc.cluster.local:9000")
    access_key = _env("MINIO_ACCESS_KEY", "minioadmin")
    secret_key = _env("MINIO_SECRET_KEY", "minioadmin")

    path_style = _env("S3A_PATH_STYLE_ACCESS", "true")
    packages = os.getenv("SPARK_S3_PACKAGES")

    builder = (
        SparkSession.builder.appName(app_name)
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.hadoop.fs.s3a.endpoint", endpoint)
        .config("spark.hadoop.fs.s3a.access.key", access_key)
        .config("spark.hadoop.fs.s3a.secret.key", secret_key)
        .config("spark.hadoop.fs.s3a.path.style.access", path_style)
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    )

    if packages:
        builder = builder.config("spark.jars.packages", packages)

    return builder.getOrCreate()
