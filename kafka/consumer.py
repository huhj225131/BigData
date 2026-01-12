from kafka import KafkaConsumer
from kafka.errors import NoBrokersAvailable
import json, time, sys, os
import traceback
import boto3
from botocore.exceptions import NoCredentialsError
from datetime import datetime

# --- CẤU HÌNH ---
# MinIO
minio_access_key = "minioadmin"
minio_secret_key = "minioadmin"
minio_endpoint = 'http://minio.minio.svc.cluster.local:9000' # Default cho test
bucket_name = 'hungluu-test-bucket' # Bucket đích để lưu data

# Kafka
KAFKA_BROKER = os.getenv('KAFKA_BROKER', 'localhost:9092')
TOPIC_NAME = 'data-stream'
GROUP_ID = "minio_archiver" # Đặt tên group riêng cho việc lưu trữ
MAX_RETRY = 10

# Batch Config
BATCH_SIZE = 10       # Số lượng tin nhắn tối đa trong 1 file
BATCH_TIMEOUT = 30    # Thời gian tối đa (giây) chờ gom batch

# Config cho kafka-python
conf = {
    'bootstrap_servers': [KAFKA_BROKER],
    'group_id': GROUP_ID,
    'auto_offset_reset': 'earliest',
    'heartbeat_interval_ms': 5000,
    'session_timeout_ms': 20000,
    'enable_auto_commit': True,
    'value_deserializer': lambda x: json.loads(x.decode('utf-8')) 
}

# --- HELPER FUNCTIONS ---

def get_s3_client():
    try:
        s3 = boto3.client(
            's3', 
            endpoint_url=minio_endpoint, 
            aws_access_key_id=minio_access_key,
            aws_secret_access_key=minio_secret_key
        )
        return s3 
    except Exception as e:
        print(f"Lỗi tạo S3 client: {e}")
        return None

def create_minio_bucket_if_not_exists(s3, bucket_name):
    try:
        s3.head_bucket(Bucket=bucket_name)
        print(f"✓ Bucket {bucket_name} đã tồn tại.")
    except:
        print(f"Bucket {bucket_name} chưa có. Đang tạo...")
        try:
            s3.create_bucket(Bucket=bucket_name)
            print(f"✓ Đã tạo bucket {bucket_name}")
        except Exception as e:
            print(f"❌ Không thể tạo bucket: {e}")
            sys.exit(1)

# Hàm upload trực tiếp từ RAM lên MinIO
def upload_batch_to_minio(s3, data_list):
    if not data_list:
        return

    # 1. Tạo tên file theo thời gian
    now = datetime.now()
    folder_path = now.strftime("data/%Y-%m-%d")
    file_name = f"{folder_path}/{int(time.time())}_{len(data_list)}.json"

    # 2. Chuyển list thành chuỗi JSON (NDJSON - mỗi dòng 1 json)
    body_content = "\n".join([json.dumps(msg) for msg in data_list])
    
    # 3. Upload dùng put_object
    try:
        s3.put_object(
            Bucket=bucket_name,
            Key=file_name,
            Body=body_content.encode('utf-8'),
            ContentType='application/json'
        )
        print(f"✅ [UPLOAD] Đã lưu {len(data_list)} tin nhắn vào: {file_name}")
    except Exception as e:
        print(f"❌ Lỗi upload MinIO: {e}")

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

# --- MAIN LOGIC ---

def process_messages(consumer, s3):
    buffer = []
    last_upload_time = time.time()
    
    print(" Bắt đầu nhận tin nhắn và gom Batch...")
    
    try:
        while True:
            # Dùng poll thay vì vòng lặp for để không bị block
            # Timeout 1000ms: mỗi 1 giây sẽ nhả ra để check logic time
            msg_pack = consumer.poll(timeout_ms=1000) 
            
            # poll trả về dict {TopicPartition: [List records]}
            for tp, messages in msg_pack.items():
                for msg in messages:
                    # msg.value đã được deserialize tự động nhờ config ở trên
                    val = msg.value 
                    buffer.append(val)
                    print(f" + Nhận msg (Buffer: {len(buffer)}/{BATCH_SIZE})")

            # --- Logic kiểm tra Batch ---
            current_time = time.time()
            is_full = len(buffer) >= BATCH_SIZE
            is_timeout = (current_time - last_upload_time) >= BATCH_TIMEOUT and len(buffer) > 0

            if is_full or is_timeout:
                trigger_reason = "FULL" if is_full else "TIMEOUT"
                print(f"⚡ Trigger Upload [{trigger_reason}]...")
                
                upload_batch_to_minio(s3, buffer)
                
                # Reset
                buffer = []
                last_upload_time = current_time

    except KeyboardInterrupt:
        print("\nĐang dừng...")
    finally:
        if buffer:
            print("Lưu nốt dữ liệu còn lại trong buffer...")
            upload_batch_to_minio(s3, buffer)
        consumer.close()

if __name__ == "__main__":
    # 1. Kết nối S3 & Kafka
    s3 = get_s3_client()
    if not s3: sys.exit(1)
    
    create_minio_bucket_if_not_exists(s3, bucket_name)
    
    consumer = create_comsumer()
    
    # 2. Chạy vòng lặp xử lý
    process_messages(consumer, s3)