import argparse
import os

from pyspark.sql import functions as F
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)

from common import build_spark


def _env(name: str, default: str) -> str:
    v = os.getenv(name)
    return v if v not in (None, "") else default


PAYLOAD_SCHEMA = StructType(
    [
        StructField("id", IntegerType()),
        StructField("price", IntegerType()),
        StructField("sqft", IntegerType()),
        StructField("bedrooms", IntegerType()),
        StructField("bathrooms", DoubleType()),
        StructField("location", StringType()),
        StructField("year_built", IntegerType()),
        StructField("condition", StringType()),
    ]
)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--bucket", default=_env("MINIO_BUCKET", "hungluu-test-bucket"))
    parser.add_argument("--bronze-prefix", default=_env("BRONZE_PREFIX", "bronze"))
    parser.add_argument("--silver-prefix", default=_env("SILVER_PREFIX", "silver"))
    parser.add_argument("--write-postgres", action="store_true")
    parser.add_argument(
        "--pg-url",
        default=_env(
            "POSTGRES_JDBC_URL",
            "jdbc:postgresql://postgres.postgres.svc.cluster.local:5432/house_warehouse",
        ),
    )
    parser.add_argument("--pg-user", default=_env("POSTGRES_USER", "postgres"))
    parser.add_argument("--pg-password", default=_env("POSTGRES_PASSWORD", "postgres"))
    parser.add_argument("--pg-table", default=_env("SILVER_PG_TABLE", "silver_house"))
    parser.add_argument(
        "--pg-mode",
        choices=["overwrite", "append"],
        default=_env("SILVER_PG_MODE", "overwrite"),
        help="How to write Silver into Postgres. overwrite is safest for demo re-runs.",
    )
    parser.add_argument(
        "--input-format",
        choices=["json", "parquet"],
        default=_env("BRONZE_INPUT_FORMAT", "json"),
        help="How bronze was written: json (NDJSON) or parquet",
    )

    args = parser.parse_args()

    spark = build_spark("silver_job")

    bronze_path = f"s3a://{args.bucket}/{args.bronze_prefix}/"
    silver_path = f"s3a://{args.bucket}/{args.silver_prefix}/"

    if args.input_format == "parquet":
        df = spark.read.parquet(bronze_path)
        if "payload" not in df.columns and "payload_json" in df.columns:
            df = df.withColumn("payload", F.from_json(F.col("payload_json"), PAYLOAD_SCHEMA))
    else:
        df = spark.read.json(bronze_path)

    if "payload" not in df.columns:
        raise RuntimeError("Bronze data must contain 'payload' struct or 'payload_json' column")

    # Flatten payload
    df = df.select(
        F.col("ingest_time_utc"),
        F.col("topic"),
        F.col("partition"),
        F.col("offset"),
        F.col("kafka_timestamp_ms"),
        F.col("payload.*"),
    )

    # Cast types + normalize
    df = (
        df.withColumn("id", F.col("id").cast(IntegerType()))
        .withColumn("price", F.col("price").cast(IntegerType()))
        .withColumn("sqft", F.col("sqft").cast(IntegerType()))
        .withColumn("bedrooms", F.col("bedrooms").cast(IntegerType()))
        .withColumn("bathrooms", F.col("bathrooms").cast(DoubleType()))
        .withColumn("year_built", F.col("year_built").cast(IntegerType()))
        .withColumn("location", F.initcap(F.lower(F.trim(F.col("location")))))
        .withColumn("condition", F.initcap(F.lower(F.trim(F.col("condition")))))
    )

    # Minimal null-handling: drop critical nulls, fill the rest
    df = df.dropna(subset=["price", "location", "year_built"]).fillna(
        {
            "sqft": 0,
            "bedrooms": 0,
            "bathrooms": 0.0,
            "condition": "Unknown",
        }
    )

    # Dedup by Kafka identity
    df = df.dropDuplicates(["topic", "partition", "offset"])

    df = df.withColumn("dt", F.to_date(F.col("ingest_time_utc")))

    (
        df.write.mode("append")
        .partitionBy("dt")
        .parquet(silver_path)
    )

    if args.write_postgres:
        props = {"user": args.pg_user, "password": args.pg_password, "driver": "org.postgresql.Driver"}
        (
            df.write.mode(args.pg_mode)
            .jdbc(
                url=args.pg_url,
                table=args.pg_table,
                properties=props,
            )
        )

    spark.stop()


if __name__ == "__main__":
    main()
