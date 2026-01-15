import argparse
import os

from pyspark.sql import functions as F

from common import build_spark


def _env(name: str, default: str) -> str:
    v = os.getenv(name)
    return v if v not in (None, "") else default


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--bucket", default=_env("MINIO_BUCKET", "hungluu-test-bucket"))
    parser.add_argument("--silver-prefix", default=_env("SILVER_PREFIX", "silver"))
    parser.add_argument("--gold-prefix", default=_env("GOLD_PREFIX", "gold"))

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

    args = parser.parse_args()

    spark = build_spark("gold_job")

    silver_path = f"s3a://{args.bucket}/{args.silver_prefix}/"
    gold_base = f"s3a://{args.bucket}/{args.gold_prefix}/"

    df = spark.read.parquet(silver_path)

    # avg price by location
    gold_location_stats = (
        df.groupBy("location")
        .agg(
            F.count("*").alias("n"),
            F.avg("price").alias("avg_price"),
            F.avg("sqft").alias("avg_sqft"),
            F.avg("bedrooms").alias("avg_bedrooms"),
            F.avg("bathrooms").alias("avg_bathrooms"),
        )
        .withColumn("as_of_utc", F.current_timestamp())
    )

    # price trend by year_built
    gold_year_trend = (
        df.groupBy("year_built")
        .agg(
            F.count("*").alias("n"),
            F.avg("price").alias("avg_price"),
        )
        .withColumn("as_of_utc", F.current_timestamp())
    )

    gold_location_path = f"{gold_base}/location_stats/"
    gold_year_path = f"{gold_base}/year_trend/"

    gold_location_stats.write.mode("overwrite").parquet(gold_location_path)
    gold_year_trend.write.mode("overwrite").parquet(gold_year_path)

    if args.write_postgres:
        props = {"user": args.pg_user, "password": args.pg_password, "driver": "org.postgresql.Driver"}

        gold_location_stats.write.mode("overwrite").jdbc(
            url=args.pg_url,
            table="gold_location_stats",
            properties=props,
        )
        gold_year_trend.write.mode("overwrite").jdbc(
            url=args.pg_url,
            table="gold_year_trend",
            properties=props,
        )

    spark.stop()


if __name__ == "__main__":
    main()
