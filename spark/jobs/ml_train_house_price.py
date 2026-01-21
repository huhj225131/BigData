import argparse
import os
from datetime import datetime, timezone

from pyspark.ml import Pipeline
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml.feature import OneHotEncoder, StringIndexer, VectorAssembler
from pyspark.ml.regression import RandomForestRegressor
from pyspark.sql import functions as F

from common import build_spark


def _env(name: str, default: str) -> str:
    v = os.getenv(name)
    return v if v not in (None, "") else default


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--bucket", default=_env("MINIO_BUCKET", "house-lake"))
    parser.add_argument("--silver-prefix", default=_env("SILVER_PREFIX", "silver"))

    parser.add_argument("--model-prefix", default=_env("MODEL_PREFIX", "models/house_price"))
    parser.add_argument(
        "--model-path",
        default=_env("MODEL_PATH", ""),
        help="Optional full s3a:// path. If set, overrides --bucket/--model-prefix.",
    )

    parser.add_argument("--seed", type=int, default=int(_env("ML_SEED", "42")))
    parser.add_argument("--train-ratio", type=float, default=float(_env("ML_TRAIN_RATIO", "0.8")))

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

    spark = build_spark("ml_train_house_price")

    spark.conf.set("spark.sql.session.timeZone", "UTC")

    silver_path = f"s3a://{args.bucket}/{args.silver_prefix}/"

    df = spark.read.parquet(silver_path)

    df = (
        df.select(
            F.col("id"),
            F.col("created_at"),
            F.col("price").cast("double").alias("label"),
            F.col("sqft").cast("double").alias("sqft"),
            F.col("bedrooms").cast("double").alias("bedrooms"),
            F.col("bathrooms").cast("double").alias("bathrooms"),
            F.col("year_built").cast("double").alias("year_built"),
            F.col("location").alias("location"),
            F.col("condition").alias("condition"),
            F.col("price_per_sqft").cast("double").alias("price_per_sqft"),
            F.col("house_age").cast("double").alias("house_age"),
            F.col("total_rooms").cast("double").alias("total_rooms"),
            F.col("condition_score").cast("double").alias("condition_score"),
        )
        .dropna(subset=["label", "location", "year_built"])
        .fillna({
            "sqft": 0.0, "bedrooms": 0.0, "bathrooms": 0.0, "condition": "Unknown",
            "price_per_sqft": 0.0, "house_age": 0.0, "total_rooms": 0.0, "condition_score": 0.0
        })
    )

    location_indexer = StringIndexer(
        inputCol="location",
        outputCol="location_idx",
        handleInvalid="keep",
    )
    condition_indexer = StringIndexer(
        inputCol="condition",
        outputCol="condition_idx",
        handleInvalid="keep",
    )

    encoder = OneHotEncoder(
        inputCols=["location_idx", "condition_idx"],
        outputCols=["location_ohe", "condition_ohe"],
        handleInvalid="keep",
    )

    assembler = VectorAssembler(
        inputCols=[
            "sqft", "bedrooms", "bathrooms", "year_built",
            "price_per_sqft", "house_age", "total_rooms", "condition_score",
            "location_ohe", "condition_ohe"
        ],
        outputCol="features",
        handleInvalid="keep",
    )

    rf = RandomForestRegressor(
        featuresCol="features",
        labelCol="label",
        predictionCol="prediction",
        seed=args.seed,
        numTrees=int(_env("RF_NUM_TREES", "50")),
        maxDepth=int(_env("RF_MAX_DEPTH", "10")),
    )

    pipeline = Pipeline(stages=[
        location_indexer, condition_indexer, encoder, assembler, rf
    ])

    train_df, test_df = df.randomSplit([args.train_ratio, 1.0 - args.train_ratio], seed=args.seed)

    model = pipeline.fit(train_df)

    pred = model.transform(test_df)

    evaluator_rmse = RegressionEvaluator(labelCol="label", predictionCol="prediction", metricName="rmse")
    evaluator_r2 = RegressionEvaluator(labelCol="label", predictionCol="prediction", metricName="r2")

    rmse = float(evaluator_rmse.evaluate(pred))
    r2 = float(evaluator_r2.evaluate(pred))

    run_id = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")

    if args.model_path:
        model_path = args.model_path.rstrip("/")
    else:
        model_path = f"s3a://{args.bucket}/{args.model_prefix.rstrip('/')}/latest"

    versioned_path = f"s3a://{args.bucket}/{args.model_prefix.rstrip('/')}/runs/{run_id}"

    model.write().overwrite().save(model_path)
    model.write().overwrite().save(versioned_path)

    metrics = spark.createDataFrame(
        [
            {
                "run_id": run_id,
                "rmse": rmse,
                "r2": r2,
                "model_path": model_path,
                "versioned_model_path": versioned_path,
                "as_of_utc": datetime.now(timezone.utc),
            }
        ]
    )

    metrics_path = f"s3a://{args.bucket}/{args.model_prefix.rstrip('/')}/metrics/"
    metrics.write.mode("append").parquet(metrics_path)

    if args.write_postgres:
        props = {"user": args.pg_user, "password": args.pg_password, "driver": "org.postgresql.Driver"}
        metrics.write.mode("append").jdbc(
            url=args.pg_url,
            table="ml_house_price_model_metrics",
            properties=props,
        )

    print(f"[ml_train_house_price] run_id={run_id} rmse={rmse:.4f} r2={r2:.4f}")
    print(f"[ml_train_house_price] model_path={model_path}")
    print(f"[ml_train_house_price] versioned_path={versioned_path}")

    spark.stop()


if __name__ == "__main__":
    main()
