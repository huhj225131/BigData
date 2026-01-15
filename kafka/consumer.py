import json
import os
import sys
import tempfile
import time
from datetime import datetime, timezone

from kafka import KafkaConsumer

from upload_to_storage import create_minio_bucket, upload_to_s3


def _env(name: str, default: str) -> str:
    v = os.getenv(name)
    return v if v not in (None, "") else default


# --- CẤU HÌNH (ENV-FIRST) ---
KAFKA_BROKER = _env("KAFKA_BROKER", "localhost:9092")
TOPIC_NAME = _env("TOPIC_NAME", "data-stream")
GROUP_ID = _env("GROUP_ID", "bronze_archiver")
MAX_RETRY = int(_env("MAX_RETRY", "10"))

MINIO_BUCKET = _env("MINIO_BUCKET", "hungluu-test-bucket")
BRONZE_PREFIX = _env("BRONZE_PREFIX", "bronze")

# batch: yêu cầu 100-500 msgs hoặc 1-5 phút
BATCH_SIZE = int(_env("BATCH_SIZE", "200"))
BATCH_TIMEOUT_SECONDS = int(_env("BATCH_TIMEOUT_SECONDS", "180"))

# output
OUTPUT_FORMAT = _env("OUTPUT_FORMAT", "jsonl").lower()  # jsonl|parquet (parquet optional)


conf = {
    "bootstrap_servers": [KAFKA_BROKER],
    "group_id": GROUP_ID,
    "auto_offset_reset": "earliest",
    "heartbeat_interval_ms": 5000,
    "session_timeout_ms": 20000,
    "enable_auto_commit": True,
    "value_deserializer": lambda x: json.loads(x.decode("utf-8")),
}

def create_comsumer():
    for attempt in range(1, 1 + MAX_RETRY):
        try:
            print(f"Kết nối Kafka (Lần {attempt})...")
            consumer = KafkaConsumer(TOPIC_NAME, **conf)
            print("✓ Kết nối Kafka thành công")
            return consumer
        except Exception as e:
            print(f"Lỗi kết nối: {e}")
            time.sleep(5)
    print("❌ Không tạo được consumer")
    sys.exit(1)

def _bronze_key(topic: str, partition: int, min_offset: int, max_offset: int, count: int, ext: str) -> str:
    now = datetime.now(timezone.utc)
    dt = now.strftime("%Y-%m-%d")
    hour = now.strftime("%H")
    ts = int(now.timestamp())
    return (
        f"{BRONZE_PREFIX}/dt={dt}/hour={hour}/topic={topic}/partition={partition}/"
        f"{ts}_offset={min_offset}-{max_offset}_count={count}.{ext}"
    )


def _write_batch_local(records: list[dict], output_format: str) -> tuple[str, str]:
    if output_format == "parquet":
        try:
            import pyarrow as pa
            import pyarrow.parquet as pq
        except Exception as e:
            raise RuntimeError(
                "OUTPUT_FORMAT=parquet requires pyarrow. "
                "Install pyarrow or set OUTPUT_FORMAT=jsonl"
            ) from e

        # keep schema stable: store payload as JSON string
        rows = []
        for r in records:
            rows.append(
                {
                    "ingest_time_utc": r.get("ingest_time_utc"),
                    "topic": r.get("topic"),
                    "partition": r.get("partition"),
                    "offset": r.get("offset"),
                    "kafka_timestamp_ms": r.get("kafka_timestamp_ms"),
                    "payload_json": json.dumps(r.get("payload"), ensure_ascii=False),
                }
            )
        table = pa.Table.from_pylist(rows)
        tmp = tempfile.NamedTemporaryFile(delete=False, suffix=".parquet")
        tmp.close()
        pq.write_table(table, tmp.name)
        return tmp.name, "parquet"

    tmp = tempfile.NamedTemporaryFile(delete=False, suffix=".jsonl", mode="w", encoding="utf-8")
    with tmp as f:
        for r in records:
            f.write(json.dumps(r, ensure_ascii=False) + "\n")
    return tmp.name, "jsonl"


def _flush_batch(records: list[dict]):
    if not records:
        return

    topic = records[0]["topic"]
    partition = records[0]["partition"]
    offsets = [r["offset"] for r in records]
    key = _bronze_key(topic, partition, min(offsets), max(offsets), len(records), "jsonl" if OUTPUT_FORMAT != "parquet" else "parquet")

    local_path, ext = _write_batch_local(records, OUTPUT_FORMAT)
    try:
        upload_to_s3(local_path, MINIO_BUCKET, key)
        print(f"✅ [BRONZE] Uploaded {len(records)} records -> s3://{MINIO_BUCKET}/{key}")
    finally:
        try:
            os.remove(local_path)
        except Exception:
            pass


def process_messages(consumer):
    buffer_by_partition: dict[tuple[str, int], list[dict]] = {}
    last_flush_time_by_partition: dict[tuple[str, int], float] = {}

    print("Bắt đầu đọc Kafka và gom batch (bronze)...")

    try:
        while True:
            msg_pack = consumer.poll(timeout_ms=1000)

            for tp, messages in msg_pack.items():
                for msg in messages:
                    k = (msg.topic, msg.partition)
                    buffer = buffer_by_partition.setdefault(k, [])
                    last_flush_time_by_partition.setdefault(k, time.time())

                    buffer.append(
                        {
                            "ingest_time_utc": datetime.now(timezone.utc).isoformat(),
                            "topic": msg.topic,
                            "partition": msg.partition,
                            "offset": msg.offset,
                            "kafka_timestamp_ms": msg.timestamp,
                            "payload": msg.value,
                        }
                    )

            now = time.time()
            for k, buffer in list(buffer_by_partition.items()):
                if not buffer:
                    continue

                is_full = len(buffer) >= BATCH_SIZE
                is_timeout = (now - last_flush_time_by_partition.get(k, now)) >= BATCH_TIMEOUT_SECONDS
                if is_full or is_timeout:
                    reason = "FULL" if is_full else "TIMEOUT"
                    print(f"⚡ Flush [{reason}] {k} ({len(buffer)} records)")
                    _flush_batch(buffer)
                    buffer_by_partition[k] = []
                    last_flush_time_by_partition[k] = now

    except KeyboardInterrupt:
        print("\nĐang dừng...")
    finally:
        for k, buffer in buffer_by_partition.items():
            if buffer:
                print(f"Flush nốt buffer {k} ({len(buffer)} records)")
                _flush_batch(buffer)
        consumer.close()

if __name__ == "__main__":
    # Ensure bucket exists (will use MINIO_* env)
    create_minio_bucket(MINIO_BUCKET)

    consumer = create_comsumer()
    process_messages(consumer)