import argparse
import os

from py4j.protocol import Py4JJavaError

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
    parser.add_argument(
        "--input-format",
        choices=["json", "parquet"],
        default=_env("BRONZE_INPUT_FORMAT", "json"),
        help="How bronze was written: json (NDJSON) or parquet",
    )

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
    parser.add_argument("--pg-table", default=_env("SILVER_PG_TABLE", "fact_house"))
    parser.add_argument("--pg-mode", choices=["append", "overwrite"], default=_env("SILVER_PG_MODE", "append"))

    parser.add_argument(
        "--dedup-strategy",
        choices=["none", "pg-max-offset"],
        default=_env("SILVER_DEDUP_STRATEGY", "pg-max-offset"),
        help="How to avoid reprocessing old Bronze data. 'pg-max-offset' uses fact_house in Postgres as state.",
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
        # NDJSON files produced by the Kafka bronze consumer
        df = spark.read.json(bronze_path)

    if "payload" not in df.columns:
        raise RuntimeError("Bronze data must contain 'payload' struct or 'payload_json' column")

    # Flatten payload (keep Kafka metadata for dedup, but we will drop them from final output)
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

    # Dedup within this batch by Kafka identity
    df = df.dropDuplicates(["topic", "partition", "offset"])

    normalized = F.regexp_replace(
        F.regexp_replace(F.col("ingest_time_utc"), "T", " "),
        r"(Z|[+-]\d\d:?\d\d)$",
        "",
    )

    created_ts = F.coalesce(
        F.to_timestamp(normalized, "yyyy-MM-dd HH:mm:ss.SSSSSS"),
        F.to_timestamp(normalized, "yyyy-MM-dd HH:mm:ss.SSS"),
        F.to_timestamp(normalized, "yyyy-MM-dd HH:mm:ss"),
    )

    created_at_from_ts = F.concat(
        F.date_format(created_ts, "yyyy-MM-dd HH:mm:ss"),
        F.lit("."),
        F.lpad(((F.unix_micros(created_ts) % F.lit(1000000))).cast("string"), 6, "0"),
    )

    frac = F.regexp_extract(normalized, r"\\.(\\d{1,6})$", 1)
    base = F.regexp_replace(normalized, r"\\.\\d{1,6}$", "")
    created_at_fallback = F.when(
        frac != "",
        F.concat(base, F.lit("."), F.lpad(frac, 6, "0")),
    ).otherwise(F.concat(normalized, F.lit(".000000")))

    created_at = F.when(created_ts.isNotNull(), created_at_from_ts).otherwise(created_at_fallback)

    df = df.withColumn("created_at", created_at)

    # Optional incremental filter: if Postgres fact table already has offsets,
    # only process records with offset > max(offset) per (topic, partition).
    if args.write_postgres and args.pg_mode == "append" and args.dedup_strategy == "pg-max-offset":
        def _quote_ident(name: str) -> str:
            return '"' + name.replace('"', '""') + '"'

        def _quote_table(ident: str) -> str:
            parts = [p for p in ident.split(".") if p]
            return ".".join(_quote_ident(p) for p in parts) if parts else _quote_ident(ident)

        table_expr = _quote_table(args.pg_table)
        max_offset_query = (
            f"(SELECT {_quote_ident('topic')} AS topic, {_quote_ident('partition')} AS partition, "
            f"MAX({_quote_ident('offset')}) AS max_offset "
            f"FROM {table_expr} GROUP BY {_quote_ident('topic')}, {_quote_ident('partition')}) t"
        )

        props = {"user": args.pg_user, "password": args.pg_password, "driver": "org.postgresql.Driver"}
        try:
            state = spark.read.jdbc(url=args.pg_url, table=max_offset_query, properties=props)

            df = (
                df.join(state, on=["topic", "partition"], how="left")
                .where((F.col("max_offset").isNull()) | (F.col("offset") > F.col("max_offset")))
                .drop("max_offset")
            )
        except Py4JJavaError:
            # Table doesn't exist yet (first run) or schema mismatch: treat as no state.
            pass

    # Silver lake schema: keep it clean (no Kafka metadata)
    df_silver = df.select(
        F.col("created_at"),
        F.col("id"),
        F.col("price"),
        F.col("sqft"),
        F.col("bedrooms"),
        F.col("bathrooms"),
        F.col("location"),
        F.col("year_built"),
        F.col("condition"),
    )

    df_silver.write.mode("append").parquet(silver_path)

    if args.write_postgres:
        # Postgres fact table: keep Kafka identity columns to support incremental processing.
        df_fact = df.select(
            F.col("topic"),
            F.col("partition"),
            F.col("offset"),
            F.col("created_at"),
            F.col("id"),
            F.col("price"),
            F.col("sqft"),
            F.col("bedrooms"),
            F.col("bathrooms"),
            F.col("location"),
            F.col("year_built"),
            F.col("condition"),
        )

        props = {"user": args.pg_user, "password": args.pg_password, "driver": "org.postgresql.Driver"}
        df_fact.write.mode(args.pg_mode).jdbc(url=args.pg_url, table=args.pg_table, properties=props)

    spark.stop()


if __name__ == "__main__":
    main()
