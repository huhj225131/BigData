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


    gold_location_stats = (
        df.groupBy("location")
        .agg(
            F.count("*").alias("total_houses"),
            F.avg("price").alias("avg_price"),
            F.stddev("price").alias("std_price"),
            F.percentile_approx("price", 0.50).alias("median_price"),
            F.min("price").alias("min_price"),
            F.max("price").alias("max_price"),
            F.avg("sqft").alias("avg_sqft"),
            F.avg("bedrooms").alias("avg_bedrooms"),
            F.avg("bathrooms").alias("avg_bathrooms"),
            F.avg("price_per_sqft").alias("avg_price_per_sqft"),
            F.avg("house_age").alias("avg_house_age"),
        )
        .withColumn("as_of_utc", F.current_timestamp())
    )

    gold_condition_stats = (
        df.groupBy("condition")
        .agg(
            F.count("*").alias("total_houses"),
            F.avg("price").alias("avg_price"),
            F.percentile_approx("price", 0.50).alias("median_price"),
            F.avg("price_per_sqft").alias("avg_price_per_sqft"),
            F.avg("house_age").alias("avg_house_age"),
            F.avg("sqft").alias("avg_sqft"),
            F.avg("condition_score").alias("avg_condition_score"),
        )
        .withColumn("as_of_utc", F.current_timestamp())
    )

    gold_bedroom_analysis = (
        df.groupBy("bedrooms")
        .agg(
            F.count("*").alias("total_houses"),
            F.avg("price").alias("avg_price"),
            F.percentile_approx("price", 0.50).alias("median_price"),
            F.min("price").alias("min_price"),
            F.max("price").alias("max_price"),
            F.avg("sqft").alias("avg_sqft"),
            F.avg("price_per_sqft").alias("avg_price_per_sqft"),
            F.avg("total_rooms").alias("avg_total_rooms"),
        )
        .filter(F.col("bedrooms") > 0)
        .orderBy("bedrooms")
        .withColumn("as_of_utc", F.current_timestamp())
    )

    gold_year_built_trends = (
        df.withColumn("decade", (F.floor(F.col("year_built") / 10) * 10).cast("int"))
        .groupBy("decade")
        .agg(
            F.count("*").alias("total_houses"),
            F.avg("price").alias("avg_price"),
            F.avg("price_per_sqft").alias("avg_price_per_sqft"),
            F.avg("sqft").alias("avg_sqft"),
            F.avg("house_age").alias("avg_age"),
            F.percentile_approx("price", 0.50).alias("median_price"),
        )
        .orderBy("decade")
        .withColumn("as_of_utc", F.current_timestamp())
    )

    gold_location_path = f"{gold_base}/location_stats/"
    gold_condition_path = f"{gold_base}/condition_stats/"
    gold_bedroom_path = f"{gold_base}/bedroom_analysis/"
    gold_year_trends_path = f"{gold_base}/year_built_trends/"

    gold_location_stats.write.mode("overwrite").parquet(gold_location_path)
    gold_condition_stats.write.mode("overwrite").parquet(gold_condition_path)
    gold_bedroom_analysis.write.mode("overwrite").parquet(gold_bedroom_path)
    gold_year_built_trends.write.mode("overwrite").parquet(gold_year_trends_path)

    if args.write_postgres:
        props = {"user": args.pg_user, "password": args.pg_password, "driver": "org.postgresql.Driver"}

        gold_location_stats.write.mode("overwrite").jdbc(
            url=args.pg_url,
            table="gold_location_stats",
            properties=props,
        )
        gold_condition_stats.write.mode("overwrite").jdbc(
            url=args.pg_url,
            table="gold_condition_stats",
            properties=props,
        )
        gold_bedroom_analysis.write.mode("overwrite").jdbc(
            url=args.pg_url,
            table="gold_bedroom_analysis",
            properties=props,
        )
        gold_year_built_trends.write.mode("overwrite").jdbc(
            url=args.pg_url,
            table="gold_year_built_trends",
            properties=props,
        )

    spark.stop()


if __name__ == "__main__":
    main()
