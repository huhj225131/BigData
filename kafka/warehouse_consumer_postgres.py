import argparse
import json
import os
import sys
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Dict, Optional, Tuple

import psycopg2
from psycopg2.extensions import connection as PgConnection
from confluent_kafka import Consumer, KafkaError


@dataclass(frozen=True)
class HouseRecord:
    id: int
    price: int
    sqft: int
    bedrooms: int
    bathrooms: float
    location: str
    year_built: int
    condition: str
    ingested_at: str

# thời gian UTC hiện tại dưới dạng ISO 8601
def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Consume house events from Kafka and upsert into a Postgres warehouse (star schema)."
    )
    # cấu hình Kafka
    parser.add_argument("--broker", default="localhost:9092")
    parser.add_argument("--topic", default="data-stream")
    parser.add_argument("--group", default="warehouse-writer-pg")

    # cấu hình Postgres
    parser.add_argument("--pg-host", default="localhost")
    parser.add_argument("--pg-port", type=int, default=5432)
    parser.add_argument("--pg-db", default="house_warehouse")
    parser.add_argument("--pg-user", default="postgres")
    parser.add_argument("--pg-password", default="postgres")

    parser.add_argument("--raw-jsonl", default=os.path.join("data", "bronze", "house_events.jsonl"))
    parser.add_argument("--poll-seconds", type=float, default=1.0)
    parser.add_argument("--max-retry", type=int, default=10)
    return parser.parse_args()

# Tạo thư mục cho output bronze
def ensure_dirs(path: str) -> None:
    parent = os.path.dirname(path)
    if parent:
        os.makedirs(parent, exist_ok=True)

# Giải mã giá trị từ bytes sang dict
def deserialize_value(value_bytes: bytes) -> Dict[str, Any]:
    return json.loads(value_bytes.decode("utf-8"))

# Convert dữ liệu sang int với kiểm tra lỗi
def to_int(value: Any, field_name: str) -> int:
    if value is None:
        raise ValueError(f"Missing field '{field_name}'")
    try:
        return int(value)
    except Exception as exc:
        raise ValueError(f"Invalid int for '{field_name}': {value!r}") from exc


def to_float(value: Any, field_name: str) -> float:
    if value is None:
        raise ValueError(f"Missing field '{field_name}'")
    try:
        return float(value)
    except Exception as exc:
        raise ValueError(f"Invalid float for '{field_name}': {value!r}") from exc


def to_str(value: Any, field_name: str) -> str:
    if value is None:
        raise ValueError(f"Missing field '{field_name}'")
    s = str(value).strip()
    if not s:
        raise ValueError(f"Empty string for '{field_name}'")
    return s

# Chuẩn hóa bản ghi thô thành HouseRecord
def normalize(record: Dict[str, Any]) -> HouseRecord:
    return HouseRecord(
        id=to_int(record.get("id"), "id"),
        price=to_int(record.get("price"), "price"),
        sqft=to_int(record.get("sqft"), "sqft"),
        bedrooms=to_int(record.get("bedrooms"), "bedrooms"),
        bathrooms=to_float(record.get("bathrooms"), "bathrooms"),
        location=to_str(record.get("location"), "location"),
        year_built=to_int(record.get("year_built"), "year_built"),
        condition=to_str(record.get("condition"), "condition"),
        ingested_at=utc_now_iso(),
    )

# Tạo consumer Kafka với cơ chế retry
def create_consumer(broker: str, group: str, max_retry: int) -> Consumer:
    conf = {
        "bootstrap.servers": broker,
        "group.id": group,
        "auto.offset.reset": "earliest", # Nếu chưa có offset đã commit, bắt đầu từ đầu
        "enable.auto.commit": False, # Tắt auto commit để kiểm soát thủ công
        "heartbeat.interval.ms": 5000,
        "session.timeout.ms": 20000,
    }

    for attempt in range(1, max_retry + 1):
        try:
            print(f"Create consumer attempt {attempt}/{max_retry}...")
            consumer = Consumer(conf)
            print(f"Connected to broker={broker}, group={group}")
            return consumer
        except Exception as e:
            print(f"Failed to create consumer: {e}")
            if attempt < max_retry:
                time.sleep(5)

    print("Cannot create consumer, exiting")
    sys.exit(1)


def connect_postgres(args: argparse.Namespace, max_retry: int) -> PgConnection:
    dsn = {
        "host": args.pg_host,
        "port": args.pg_port,
        "dbname": args.pg_db,
        "user": args.pg_user,
        "password": args.pg_password,
    }

    for attempt in range(1, max_retry + 1):
        try:
            print(f"Connect Postgres attempt {attempt}/{max_retry}...")
            conn = psycopg2.connect(**dsn)
            conn.autocommit = False
            print(f"Connected to Postgres {args.pg_host}:{args.pg_port}/{args.pg_db}")
            return conn
        except Exception as e:
            print(f"Failed to connect Postgres: {e}")
            if attempt < max_retry:
                time.sleep(5)

    print("Cannot connect Postgres, exiting")
    sys.exit(1)

# Khởi tạo schema kho dữ liệu 
def init_warehouse(conn: PgConnection) -> None:
    ddl = """
    CREATE TABLE IF NOT EXISTS dim_location (
        location_id SERIAL PRIMARY KEY,
        location_name TEXT NOT NULL UNIQUE
    );

    CREATE TABLE IF NOT EXISTS dim_condition (
        condition_id SERIAL PRIMARY KEY,
        condition_name TEXT NOT NULL UNIQUE
    );

    CREATE TABLE IF NOT EXISTS fact_house (
        house_id INTEGER PRIMARY KEY,
        location_id INTEGER NOT NULL REFERENCES dim_location(location_id),
        condition_id INTEGER NOT NULL REFERENCES dim_condition(condition_id),
        price INTEGER NOT NULL,
        sqft INTEGER NOT NULL,
        bedrooms INTEGER NOT NULL,
        bathrooms DOUBLE PRECISION NOT NULL,
        year_built INTEGER NOT NULL,
        ingested_at TIMESTAMPTZ NOT NULL,
        kafka_topic TEXT,
        kafka_partition INTEGER,
        kafka_offset BIGINT
    );

    CREATE OR REPLACE VIEW gold_location_stats AS
    SELECT
        l.location_name AS location,
        COUNT(*) AS n,
        AVG(f.price)::DOUBLE PRECISION AS avg_price,
        AVG(f.sqft)::DOUBLE PRECISION AS avg_sqft,
        AVG(f.price::DOUBLE PRECISION / NULLIF(f.sqft, 0)) AS avg_price_per_sqft
    FROM fact_house f
    JOIN dim_location l ON l.location_id = f.location_id
    GROUP BY l.location_name;
    """

    with conn.cursor() as cur:
        cur.execute(ddl)
    conn.commit()

# Thêm bản ghi thô vào file JSONL
def append_raw_jsonl(path: str, raw_record: Dict[str, Any], kafka_meta: Dict[str, Any]) -> None:
    ensure_dirs(path)
    line = {
        **raw_record,
        "_kafka": kafka_meta,
        "_ingested_at": utc_now_iso(),
    }
    with open(path, "a", encoding="utf-8") as f:
        f.write(json.dumps(line, ensure_ascii=False) + "\n")


def kafka_meta(msg) -> Dict[str, Any]:
    return {
        "topic": msg.topic(),
        "partition": msg.partition(),
        "offset": msg.offset(),
        "timestamp": msg.timestamp(),
    }

# Lấy id của dim_location và dim_condition
def get_dim_ids(conn: PgConnection, location: str, condition: str) -> Tuple[int, int]:
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO dim_location (location_name)
            VALUES (%s)
            ON CONFLICT (location_name) DO UPDATE SET location_name = EXCLUDED.location_name
            RETURNING location_id;
            """,
            (location,),
        )
        location_id = int(cur.fetchone()[0])

        cur.execute(
            """
            INSERT INTO dim_condition (condition_name)
            VALUES (%s)
            ON CONFLICT (condition_name) DO UPDATE SET condition_name = EXCLUDED.condition_name
            RETURNING condition_id;
            """,
            (condition,),
        )
        condition_id = int(cur.fetchone()[0])

    return location_id, condition_id

def upsert_fact(conn: PgConnection, row: HouseRecord, msg) -> None:
    meta = kafka_meta(msg) # Lấy meta từ Kafka message
    location_id, condition_id = get_dim_ids(conn, row.location, row.condition) # Lấy từ dim

    # insert hoặc cập nhật bản ghi fact_house
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO fact_house (
                house_id, location_id, condition_id,
                price, sqft, bedrooms, bathrooms, year_built,
                ingested_at, kafka_topic, kafka_partition, kafka_offset
            ) VALUES (
                %s, %s, %s,
                %s, %s, %s, %s, %s,
                %s, %s, %s, %s
            )
            ON CONFLICT (house_id) DO UPDATE SET
                location_id = EXCLUDED.location_id,
                condition_id = EXCLUDED.condition_id,
                price = EXCLUDED.price,
                sqft = EXCLUDED.sqft,
                bedrooms = EXCLUDED.bedrooms,
                bathrooms = EXCLUDED.bathrooms,
                year_built = EXCLUDED.year_built,
                ingested_at = EXCLUDED.ingested_at,
                kafka_topic = EXCLUDED.kafka_topic,
                kafka_partition = EXCLUDED.kafka_partition,
                kafka_offset = EXCLUDED.kafka_offset;
            """,
            (
                row.id,
                location_id,
                condition_id,
                row.price,
                row.sqft,
                row.bedrooms,
                row.bathrooms,
                row.year_built,
                row.ingested_at,
                meta.get("topic"),
                meta.get("partition"),
                meta.get("offset"),
            ),
        )

# Xử lý message từ Kafka
def process_message(conn: PgConnection, raw_jsonl_path: str, msg) -> Optional[HouseRecord]:
    raw = deserialize_value(msg.value())
    append_raw_jsonl(raw_jsonl_path, raw, kafka_meta(msg))

    try:
        row = normalize(raw)
    except Exception as e:
        print(f"Skip invalid record at offset={msg.offset()}: {e}")
        return None

    upsert_fact(conn, row, msg)
    return row


def main() -> int:
    args = parse_args()

    ensure_dirs(args.raw_jsonl)

    consumer = create_consumer(args.broker, args.group, args.max_retry)
    consumer.subscribe([args.topic])

    conn = connect_postgres(args, args.max_retry)
    init_warehouse(conn)

    print(f"Subscribed topic={args.topic}")
    print(f"Bronze raw JSONL={args.raw_jsonl}")
    print(
        f"Postgres={args.pg_host}:{args.pg_port}/{args.pg_db} user={args.pg_user} schema=dim_*/fact_house"
    )

    try:
        while True:
            msg = consumer.poll(args.poll_seconds)
            if msg is None:
                continue

            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                print(f"❌ Kafka error: {msg.error()}")
                continue

            try:
                row = process_message(conn, args.raw_jsonl, msg)
                conn.commit()
            except Exception as e:
                conn.rollback()
                print(f"❌ Write failed at offset={msg.offset()}: {e}")
                continue

            if row is not None:
                print(
                    f"[offset={msg.offset()}] upsert house_id={row.id} price={row.price} location={row.location}"
                )

            # Commit offset sau khi xử lý thành công
            consumer.commit(message=msg, asynchronous=False)

    except KeyboardInterrupt:
        print("\nStopping...")
    finally:
        try:
            conn.close()
        except Exception:
            pass
        consumer.close()

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
