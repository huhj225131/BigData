# Gửi dữ liệu nhà mà k set interval 
import argparse
import json
from pathlib import Path

from confluent_kafka import Producer


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Send first N house records from a JSON array file to Kafka.")
    p.add_argument("--broker", default="localhost:9092")
    p.add_argument("--topic", default="data-stream")
    p.add_argument("--file", default=str(Path(__file__).resolve().parents[1] / "house_data.json"))
    p.add_argument("--n", type=int, default=200)
    return p.parse_args()


def main() -> int:
    args = parse_args()
    file_path = Path(args.file)

    data = json.loads(file_path.read_text(encoding="utf-8"))
    if not isinstance(data, list):
        raise SystemExit("house_data.json must be a JSON array")

    producer = Producer({"bootstrap.servers": args.broker})

    sent = 0

    def dr(err, msg):
        nonlocal sent
        if err is None:
            sent += 1

    for record in data[: max(args.n, 0)]:
        key = str(record.get("id", sent)).encode("utf-8")
        value = json.dumps(record, ensure_ascii=False).encode("utf-8")
        producer.produce(args.topic, key=key, value=value, callback=dr)

    producer.flush(30)
    print(f"sent={sent} requested={min(len(data), max(args.n, 0))}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
