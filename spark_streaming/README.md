# Spark Streaming - Real-time Processing

Xử lý dữ liệu streaming từ Kafka và ghi vào PostgreSQL

## Kiến trúc Luồng 2

```
Kafka (data-stream) → Spark Streaming → PostgreSQL (house_data_final)
```

## Triển khai

### 1. Chuẩn bị PostgreSQL

Đảm bảo PostgreSQL local đã chạy và tạo table:

```sql
CREATE TABLE IF NOT EXISTS house_data_final (
    id INTEGER,
    price DOUBLE PRECISION,
    sqft INTEGER,
    bedrooms INTEGER,
    bathrooms DOUBLE PRECISION,
    location VARCHAR(255),
    year_built INTEGER,
    condition VARCHAR(50),
    is_expensive VARCHAR(10),
    processed_by VARCHAR(20),
    created_at TIMESTAMP
);
```

### 2. Deploy PostgreSQL ExternalName Service

```powershell
# Deploy service kết nối PostgreSQL local
minikube kubectl -- apply -f ..\postgres-external.yaml
```

### 3. Deploy Spark Streaming

```powershell
# Deploy Spark Streaming application
minikube kubectl -- apply -f spark-deployment.yaml -n kafka

# Kiểm tra trạng thái
minikube kubectl -- get pods -n kafka -l app=spark-streaming
```

### 4. Copy code vào Pod

```powershell
# Lấy tên Pod
$POD = (minikube kubectl -- get pods -n kafka -l app=spark-streaming -o jsonpath='{.items[0].metadata.name}')

# Copy file stream.py vào Pod
minikube kubectl -- cp stream.py kafka/${POD}:/app/stream.py

# Restart pod để áp dụng code mới
minikube kubectl -- rollout restart deployment/spark-streaming -n kafka
```

### 5. Xem logs

```powershell
# Xem logs real-time
minikube kubectl -- logs -f deployment/spark-streaming -n kafka

# Xem logs trước đó
minikube kubectl -- logs deployment/spark-streaming -n kafka --tail=100
```

## Cấu hình

Chỉnh sửa ConfigMap trong [spark-deployment.yaml](spark-deployment.yaml):

- **KAFKA_BROKER**: Địa chỉ Kafka broker
- **KAFKA_TOPIC**: Tên topic đọc dữ liệu
- **POSTGRES_HOST**: Hostname PostgreSQL (postgres-local qua ExternalName)
- **POSTGRES_DB**: Database name (house_warehouse)
- **POSTGRES_TABLE**: Tên bảng ghi dữ liệu
- **CHECKPOINT_DIR**: Thư mục checkpoint (mounted từ PVC)

## Troubleshooting

### Pod không start
```powershell
# Kiểm tra events
minikube kubectl -- describe pod -l app=spark-streaming -n kafka

# Kiểm tra logs
minikube kubectl -- logs -l app=spark-streaming -n kafka
```

### Không kết nối được PostgreSQL
```powershell
# Test kết nối từ trong pod
minikube kubectl -- exec -it deployment/spark-streaming -n kafka -- bash
# Trong pod:
telnet postgres-local 5432
```

### Checkpoint errors
```powershell
# Xóa checkpoint để reset
minikube kubectl -- exec -it deployment/spark-streaming -n kafka -- rm -rf /checkpoint/*
```

## Monitoring

### Kiểm tra dữ liệu trong PostgreSQL

```sql
-- Số lượng records
SELECT COUNT(*) FROM house_data_final WHERE processed_by = 'streaming';

-- Records mới nhất
SELECT * FROM house_data_final 
WHERE processed_by = 'streaming' 
ORDER BY created_at DESC 
LIMIT 10;

-- Thống kê theo giờ
SELECT 
    DATE_TRUNC('hour', created_at) as hour,
    COUNT(*) as count
FROM house_data_final 
WHERE processed_by = 'streaming'
GROUP BY hour 
ORDER BY hour DESC;
```

## Cập nhật code

Sau khi sửa [stream.py](stream.py):

```powershell
$POD = (minikube kubectl -- get pods -n kafka -l app=spark-streaming -o jsonpath='{.items[0].metadata.name}')
minikube kubectl -- cp stream.py kafka/${POD}:/app/stream.py
minikube kubectl -- rollout restart deployment/spark-streaming -n kafka
```
