from confluent_kafka import Consumer,KafkaError
import json, time, sys
KAFKA_BROKER = "localhost:9092"
TOPIC_NAME = 'data-stream'
GROUP_ID = "reader"
MAX_RETRY = 10

conf = {
    'bootstrap.servers': KAFKA_BROKER,
    "group.id": GROUP_ID,
    "auto.offset.reset":"earliest",
    "heartbeat.interval.ms":5000,
    "session.timeout.ms":20000,
    "enable.auto.commit": False
}

def deserialize_value(v):
    return json.loads(v.decode('utf-8'))

def create_comsumer():
    for attempt in range(1, 1 + MAX_RETRY):
        try:
            print(f"Lần tạo thứ {attempt}")
            consumer = Consumer(conf)
            print("Kết nối Kafka thành công")
            print(f"✓ Consumer group: {GROUP_ID}")
            return consumer
        except Exception as e:
            print(f"Lần thử {attempt/MAX_RETRY} không thành công")
            print(f"Lỗi: {e}")
            if attempt < MAX_RETRY:
                print("Chờ 5s kết nối lại")
                time.sleep(5)
    print("Không tạo được consumer")
    sys.exit(1)


def receive_message(consumer:Consumer, topics:list):
    if not isinstance(topics, list):
        print(f"Topic cần dạng list, nhận được dạng {type(topics)}")
        sys.exit(1)
    try:
        consumer.subscribe(topics=topics)
        while True:
          
            msg = consumer.poll(1.0)

            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                else:
                    print(f"❌ Lỗi Kafka: {msg.error()}")
                    continue

           
            val = deserialize_value(msg.value())
            
            # In ra màn hình (hoặc đẩy đi DB/API ở đây)
            print(f" [Offset {msg.offset()}] Data: {val}")

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