from kafka import KafkaProducer
from kafka.errors import KafkaError, NoBrokersAvailable
import json, sys, time, os

# --- CẤU HÌNH ---
KAFKA_BROKER = os.getenv('KAFKA_BROKER', 'localhost:9092')
TOPIC_NAME = 'data-stream'
DATA_FILE = 'kafka/house_data.json'
INTERVAL = 3
# DATA_FILE = '/app/house_data.json'
# INTERVAL = int(os.getenv('INTERVAL', '3'))
MAX_RECORDS = int(os.getenv('MAX_RECORDS', '0'))  # 0 = send all
REPEAT = os.getenv('REPEAT', '0').strip().lower() in ('1', 'true', 'yes', 'y')
MAX_RETRY = 10

# --- CÁC HÀM XỬ LÝ ---

def on_send_success(record_metadata):
    """Callback khi gửi thành công"""
    print(f"✓ Gửi thành công tới {record_metadata.topic} [{record_metadata.partition}] @ offset {record_metadata.offset}")

def on_send_error(excp):
    """Callback khi gửi thất bại"""
    print(f"✗ Gửi thất bại: {excp}")

def create_producer():
    """Tạo KafkaProducer với cơ chế Retry"""
    for attempt in range(1, MAX_RETRY + 1):
        try:
            print(f"Lần thử kết nối thứ {attempt} tới {KAFKA_BROKER}...")
            
            producer = KafkaProducer(
                bootstrap_servers=[KAFKA_BROKER],
                # Tự động serialize JSON và Encode UTF-8 tại đây
                value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                key_serializer=lambda k: str(k).encode('utf-8'),
                # Các cấu hình khác (đổi dấu chấm thành gạch dưới)
                acks='all',
                retries=5,
                request_timeout_ms=120000,
                metadata_max_age_ms=300000,
                # api_version=(0, 10, 1) # Bật dòng này nếu Kafka quá cũ hoặc không tự nhận diện version
            )
            
            print("✓ Tạo producer thành công")
            return producer
            
        except NoBrokersAvailable:
            print(f"✗ Không tìm thấy Broker nào. Đang thử lại...")
        except Exception as e:
            print(f"✗ Lỗi kết nối: {e}")
            
        if attempt < MAX_RETRY:
            print(f"  Đợi 5 giây...")
            time.sleep(5)
            
    print("✗ KHÔNG THỂ KẾT NỐI KAFKA. DỪNG CHƯƠNG TRÌNH.")
    sys.exit(1)

def load_data(file_path):
    """Đọc dữ liệu từ file JSON (Giữ nguyên logic cũ)"""
    print(f"\nĐang đọc dữ liệu từ: {file_path}")
    
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            data = json.load(f)
        
        if not isinstance(data, list):
            print(f"File JSON phải có dạng list, không phải {type(data)}!")
            sys.exit(1)
            
        print(f"✓ Đã đọc {len(data)} records từ file")
        return data
        
    except FileNotFoundError:
        print(f"  Không tìm thấy file: {file_path}")
        print(f"  Hãy copy file vào pod bằng lệnh:")
        print(f"  kubectl cp house_data.json kafka/<tên-pod>:{file_path}")
        sys.exit(1)
        
    except json.JSONDecodeError as e:
        print(f"✗ Lỗi format JSON: {e}")
        sys.exit(1)

def send_message(producer, key, value, sleep_time):
    try:
        # Trong kafka-python, send() trả về một Future
        future = producer.send(TOPIC_NAME, key=key, value=value)
        
        # Gắn callback
        future.add_callback(on_send_success)
        future.add_errback(on_send_error)
        
        # Đợi một chút để giả lập stream
        time.sleep(sleep_time)
        
    except Exception as e:
        print(f"✗ Lỗi khi gửi: {e}")

# --- MAIN ---
if __name__ == "__main__":
    # 1. Khởi tạo kết nối
    producer = create_producer()
    
    # 2. Đọc dữ liệu
    data = load_data(DATA_FILE)
    
    print(f"Bắt đầu gửi dữ liệu vào topic '{TOPIC_NAME}'...")
    
    total_sent = 0
    loop_no = 0

    try:
        while True:
            loop_no += 1
            for i, record in enumerate(data):
                if MAX_RECORDS > 0 and total_sent >= MAX_RECORDS:
                    raise StopIteration

                # Lấy key (ưu tiên cột 'id', nếu không có thì lấy số thứ tự)
                key = record.get("id", str(i))

                total_sent += 1
                print(f"\n[Record #{total_sent}] (loop={loop_no})")
                send_message(
                    producer=producer,
                    key=key,
                    value=record,
                    sleep_time=INTERVAL
                )

            if not REPEAT:
                break

    except (KeyboardInterrupt, StopIteration):
        print("\nĐã dừng.")
        
    finally:
        print("Đang flush dữ liệu còn sót...")
        producer.flush()
        producer.close()
        print("Đã đóng kết nối.")