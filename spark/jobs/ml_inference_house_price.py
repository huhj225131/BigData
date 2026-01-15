import argparse
import os
from datetime import datetime, timezone

from pyspark.ml import PipelineModel
from pyspark.sql import functions as F

from common import build_spark


def _env(name: str, default: str) -> str:
    v = os.getenv(name)
    return v if v not in (None, "") else default


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--bucket", default=_env("MINIO_BUCKET", "house-lake"))
    parser.add_argument("--silver-prefix", default=_env("SILVER_PREFIX", "silver"))
    parser.add_argument("--gold-prefix", default=_env("GOLD_PREFIX", "gold"))

    parser.add_argument(
        "--model-path",
        default=_env("MODEL_PATH", ""),
        help="Full s3a:// path to a saved Spark PipelineModel. Defaults to s3a://<bucket>/models/house_price/latest",
    )

    parser.add_argument(
        "--dt",
        default=_env("INFER_DT", ""),
        help="Optional dt filter (YYYY-MM-DD). If not set, scores all available silver partitions.",
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

    args = parser.parse_args()

    spark = build_spark("ml_inference_house_price")

    silver_path = f"s3a://{args.bucket}/{args.silver_prefix}/"

    df = spark.read.parquet(silver_path)

    if args.dt:
        df = df.filter(F.col("dt") == F.to_date(F.lit(args.dt)))

    df = df.select(
        F.col("ingest_time_utc"),
        F.col("topic"),
        F.col("partition"),
        F.col("offset"),
        F.col("id"),
        F.col("price").cast("double").alias("actual_price"),
        F.col("sqft").cast("double").alias("sqft"),
        F.col("bedrooms").cast("double").alias("bedrooms"),
        F.col("bathrooms").cast("double").alias("bathrooms"),
        F.col("year_built").cast("double").alias("year_built"),
        F.col("location"),
        F.col("condition"),
        F.col("dt"),
    )

    df = df.dropna(subset=["location", "year_built"]).fillna(
        {"sqft": 0.0, "bedrooms": 0.0, "bathrooms": 0.0, "condition": "Unknown"}
    )

    model_path = args.model_path.strip() or f"s3a://{args.bucket}/models/house_price/latest"
    model = PipelineModel.load(model_path)

    scored = model.transform(df)

    run_id = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")

    out = (
        scored.select(
            "ingest_time_utc",
            "topic",
            "partition",
            "offset",
            "id",
            "dt",
            "actual_price",
            F.col("prediction").cast("double").alias("predicted_price"),
        )
        .withColumn("model_path", F.lit(model_path))
        .withColumn("run_id", F.lit(run_id))
        .withColumn("as_of_utc", F.current_timestamp())
    )

    # Write to MinIO (gold)
    pred_path = f"s3a://{args.bucket}/{args.gold_prefix}/predictions_house_price/"
    out.write.mode("append").partitionBy("dt").parquet(pred_path)

    if args.write_postgres:
        props = {"user": args.pg_user, "password": args.pg_password, "driver": "org.postgresql.Driver"}
        out.write.mode("append").jdbc(
            url=args.pg_url,
            table="house_price_predictions",
            properties=props,
        )

    print(f"[ml_inference_house_price] run_id={run_id} model_path={model_path}")
    print(f"[ml_inference_house_price] wrote predictions to {pred_path}")

    spark.stop()


if __name__ == "__main__":
    main()
