from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, when, expr, initcap, lower, trim, lit
from pyspark.sql.types import StructType, StringType, IntegerType, DoubleType
import os

KAFKA_BROKER = os.getenv("KAFKA_BROKER", "kafka-service.kafka.svc.cluster.local:9092")
TOPIC_NAME = os.getenv("KAFKA_TOPIC", "data-stream")

POSTGRES_HOST = os.getenv("POSTGRES_HOST", "postgres.postgres.svc.cluster.local")
POSTGRES_PORT = os.getenv("POSTGRES_PORT", "5432")
POSTGRES_DB = os.getenv("POSTGRES_DB", "house_warehouse")
POSTGRES_USER = os.getenv("POSTGRES_USER", "postgres")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "postgres")

CHECKPOINT_DIR = os.getenv("CHECKPOINT_DIR", "/tmp/spark-checkpoint")

spark = SparkSession.builder \
    .appName("HouseDataStreamingCleaner") \
    .config("spark.jars.packages", 
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,"
            "org.postgresql:postgresql:42.7.1") \
    .config("spark.sql.session.timeZone", "Asia/Ho_Chi_Minh") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

schema = StructType() \
    .add("id", IntegerType()) \
    .add("price", DoubleType()) \
    .add("sqft", IntegerType()) \
    .add("bedrooms", IntegerType()) \
    .add("bathrooms", DoubleType()) \
    .add("location", StringType()) \
    .add("year_built", IntegerType()) \
    .add("condition", StringType())

raw_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_BROKER) \
    .option("subscribe", TOPIC_NAME) \
    .load()

df = raw_df.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), schema).alias("data")) \
    .select("data.*")

cleaned_df = df.na.fill({
    "price": 0.0,
    "sqft": 0,
    "location": "Unknown",
    "condition": "Good"
})

cleaned_df = cleaned_df \
    .withColumn("location", initcap(lower(trim(col("location"))))) \
    .withColumn("condition", initcap(lower(trim(col("condition")))))


db_properties = {
    "user": POSTGRES_USER,
    "password": POSTGRES_PASSWORD,
    "driver": "org.postgresql.Driver"
}
jdbc_url = f"jdbc:postgresql://{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}"

TARGET_TABLE = os.getenv("POSTGRES_TABLE", "house_data_speed") 

def write_to_postgres(df, epoch_id):
    try:
        df_with_source = df.withColumn("created_at", expr("current_timestamp()"))
        count = df_with_source.count()
        if count > 0:
            df_with_source.write.jdbc(
                url=jdbc_url, 
                table=TARGET_TABLE,
                mode="append", 
                properties=db_properties
            )
            print(f"[Batch {epoch_id}] Đã ghi {count} records vào PostgreSQL")
        else:
            print(f"[Batch {epoch_id}] Không có dữ liệu để ghi")
    except Exception as e:
        print(f"[Batch {epoch_id}] Lỗi ghi PostgreSQL: {e}")
        raise

print(f"   Khởi động Spark Streaming...")
print(f"   Kafka: {KAFKA_BROKER}")
print(f"   Topic: {TOPIC_NAME}")
print(f"   PostgreSQL: {jdbc_url}")
print(f"   Checkpoint: {CHECKPOINT_DIR}")

query = cleaned_df.writeStream \
    .foreachBatch(write_to_postgres) \
    .option("checkpointLocation", CHECKPOINT_DIR) \
    .trigger(processingTime='5 seconds') \
    .start()

print("✓ Spark Streaming đang chạy... Nhấn Ctrl+C để dừng.")
query.awaitTermination()