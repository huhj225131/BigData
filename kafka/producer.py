from confluent_kafka import Producer
import json,sys,time
KAFKA_BROKER = "localhost:9092"
TOPIC_NAME = 'data-stream'
DATA_FILE = '../house_data.json' 
INTERVAL = 5
MAX_RETRY = 10


conf = {
    'bootstrap.servers': KAFKA_BROKER,
    'acks': 'all', 
    'retries': 5,
    'request.timeout.ms': 120000,      
    'metadata.max.age.ms': 300000,     
    'socket.timeout.ms': 120000,      
    'message.timeout.ms': 120000,      
}
def serialize_value(v):
    return json.dumps(v).encode('utf-8')

def serialize_key(k):
    return str(k).encode('utf-8')

def delivery_report(err, msg):
    if err is not None:
        print(f"✗ Gửi thất bại: {err}")
    else:
        print(f"✓ Gửi thành công tới {msg.topic()} [{msg.partition()}] @ offset {msg.offset()}")

def create_producer():
    for attempt in range(1, MAX_RETRY + 1):
        try:
            print(f"Lần thử kết nối thứ {attempt}")
            producer = Producer(conf)
            print("Tạo producer thành công")
            print(f"Kết nối Kafka broker: {KAFKA_BROKER}")
            return producer
        except Exception as e:
            print(f"✗ Lần thử {attempt}/{MAX_RETRY} thất bại: {e}")
            print(f"Lỗi: {e}")
            if attempt < MAX_RETRY:
                print(f"  Đợi 5 giây trước khi thử lại...")
                time.sleep(5)
    print("Không tạo được kết nối với kafka")
    sys.exit(1)

def send_message(producer,key,  value,sleep_time):
    try:
        producer.produce(
            topic=TOPIC_NAME,
            key=serialize_key(key),
            value=serialize_value(value),
            callback=delivery_report
        )
        producer.poll(0)
        time.sleep(sleep_time)
    except BufferError:
        print("✗ Local queue full")

def load_data(file_path):
    """Đọc dữ liệu từ file JSON"""
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
        print(f"  Hãy copy file data vào pod bằng lệnh:")
        print(f"  minikube kubectl -- cp your_data.json kafka/my-producer:/data.json")
        sys.exit(1)
        
    except json.JSONDecodeError as e:
        print(f" Lỗi đọc JSON: {e}")
        print(f"  File phải có format JSON hợp lệ!git")
        sys.exit(1)
        
    except Exception as e:
        print(f" Lỗi không xác định: {e}")
        sys.exit(1)

if __name__ == "__main__":
    print("Tạo producer")
    producer = create_producer()
    print("Đọc dữ liệu")
    data = load_data(DATA_FILE)
    print("Bắt đầu gửi dữ liệu ")
    try:
        for i, record in enumerate(data):
            print(f"\n[Record #{i+1}]")
            
            # Kiểm tra xem có field 'id' không, nếu không lấy index làm key
            key = record.get("id", str(i))
            
            send_message(
                producer=producer, 
                key=key, 
                value=record, 
                sleep_time=INTERVAL
            )
            
    except KeyboardInterrupt:
        print("\nĐã dừng thủ công.")
        
    finally:
        print("Đang flush dữ liệu còn sót...")
        producer.flush(10)
        print("Đã đóng kết nối.")




