from kafka import KafkaConsumer
from kafka.errors import NoBrokersAvailable
import json, time, sys, os

# --- CẤU HÌNH ---
# Sử dụng biến môi trường hoặc default localhost
KAFKA_BROKER = os.getenv('KAFKA_BROKER', 'http://localhost:9092')
TOPIC_NAME = 'data-stream'
GROUP_ID = "new_reader_1"
MAX_RETRY = 10

# Config cho kafka-python (lưu ý dùng gạch dưới _ thay vì dấu chấm .)
conf = {
    'bootstrap_servers': [KAFKA_BROKER],
    'group_id': GROUP_ID,
    'auto_offset_reset': 'earliest',
    'heartbeat_interval_ms': 5000,
    'session_timeout_ms': 20000,
    'enable_auto_commit': True
}

def deserialize_value(v):
    # msg.value trong kafka-python là bytes, cần decode
    return json.loads(v.decode('utf-8'))

def create_comsumer():
    for attempt in range(1, 1 + MAX_RETRY):
        try:
            print(f"Lần tạo thứ {attempt}")
            # Khởi tạo KafkaConsumer với **conf để unpack dict thành tham số
            consumer = KafkaConsumer(**conf)
            print("Kết nối Kafka thành công")
            print(f"✓ Consumer group: {GROUP_ID}")
            return consumer
        except Exception as e:
            print(f"Lần thử {attempt}/{MAX_RETRY} không thành công")
            print(f"Lỗi: {e}")
            if attempt < MAX_RETRY:
                print("Chờ 5s kết nối lại")
                time.sleep(5)
    print("Không tạo được consumer")
    sys.exit(1)


def receive_message(consumer: KafkaConsumer, topics: list):
    if not isinstance(topics, list):
        print(f"Topic cần dạng list, nhận được dạng {type(topics)}")
        sys.exit(1)
    try:
        consumer.subscribe(topics=topics)
        
        # kafka-python hỗ trợ vòng lặp trực tiếp (tương đương while True + poll)
        for msg in consumer:
            # msg.value là bytes, gọi hàm deserialize của bạn
            val = deserialize_value(msg.value)
            
            # msg.offset là thuộc tính (không cần dấu ngoặc)
            print(f" [Offset {msg.offset}] Data: {val}")

    except KeyboardInterrupt:
        print("\n Đang dừng consumer...")
    except Exception as e:
        print(f" Lỗi không mong muốn: {e}")
    finally:
        print("Đóng kết nối Kafka.")
        consumer.close()
    
if __name__ == "__main__":
    consumer = create_comsumer()
    receive_message(consumer, ['data-stream'])