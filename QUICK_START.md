# 🚀 QUICK START - BigData Pipeline

**Lambda Architecture:** Batch Layer (CronJob) + Speed Layer (Streaming) chạy tự động

---

## 📋 Prerequisites

- ✅ Minikube running (`minikube start`)
- ✅ kubectl configured
- ✅ Đang ở thư mục root: `D:\2025.1\bigdata\btl_bigdata`
- ✅ MinIO bucket `house-lake` đã tạo

---

## 🏗️ BƯỚC 1: Deploy Infrastructure (1 lần)

```powershell
# 1.1. MinIO
kubectl create namespace minio
kubectl apply -f minio/config_minio.yaml
kubectl wait --for=condition=Ready pod -l app=minio -n minio --timeout=90s

# 1.2. PostgreSQL
kubectl apply -f postgres/postgres.yaml
kubectl wait --for=condition=Ready pod -l app=postgres -n postgres --timeout=90s

# 1.3. Kafka
kubectl create namespace kafka
kubectl apply -f kafka/kafka.yaml
kubectl wait --for=condition=Ready pod/kafka-0 -n kafka --timeout=120s

# 1.4. Spark namespace (cho CronJobs)
kubectl create namespace spark

# 1.5. Kafka Flow (Producer + Consumer + Spark Streaming)
kubectl apply -f kafka/flow.yaml
kubectl apply -f spark_streaming/spark-deployment.yaml
Start-Sleep -Seconds 30

# Verify
kubectl get pods -A | Select-String -Pattern "(kafka|minio|postgres|spark)"
```

**Expected: Tất cả pods `Running`**

---

## 🔧 BƯỚC 2: Copy Code vào Kafka Pods

```powershell
# 2.1. Producer
$POD_PRO = kubectl get pods -n kafka -l app=producer -o jsonpath='{.items[0].metadata.name}'
kubectl cp kafka/producer.py -n kafka "${POD_PRO}:/app/producer.py"
kubectl cp kafka/house_data.json -n kafka "${POD_PRO}:/app/house_data.json"

# 2.2. Consumer
$POD_CON = kubectl get pods -n kafka -l app=consumer -o jsonpath='{.items[0].metadata.name}'
kubectl cp kafka/consumer.py -n kafka "${POD_CON}:/app/consumer.py"
kubectl cp kafka/upload_to_storage.py -n kafka "${POD_CON}:/app/upload_to_storage.py"

# 2.3. Spark Streaming
$POD_STREAM = kubectl get pods -n kafka -l app=spark-streaming -o jsonpath='{.items[0].metadata.name}'
kubectl cp spark_streaming/stream.py -n kafka "${POD_STREAM}:/app/stream.py"

Write-Host "✅ Code copied!" -ForegroundColor Green
```

---

## 🎯 BƯỚC 3: Tạo MinIO Bucket

```powershell
# Terminal riêng - Port-forward MinIO
kubectl -n minio port-forward svc/minio-public 9001:9001
```

**Mở browser:**
- URL: `http://localhost:9001`
- Login: `minioadmin` / `minioadmin`
- Tạo bucket tên: **`house-lake`**

---

## 🚀 BƯỚC 4: Deploy Spark Pipeline (Chạy ngay + Auto schedule)

```powershell
# Deploy Batch Pipeline + ML Pipeline (sẽ chạy ngay lập tức)
kubectl apply -f spark/batch-pipeline-cronjob.yaml
kubectl apply -f spark/house-price-train-job.yaml

# Xem jobs đang chạy
kubectl get jobs -n spark
# OUTPUT:
# batch-pipeline-init    0/1    5s
# ml-train-init          0/1    3s

# Xem logs real-time
Write-Host "Watching Batch Pipeline Init..." -ForegroundColor Cyan
kubectl logs -n spark -l job-name=batch-pipeline-init --tail=100 -f

# Sau khi Batch xong, xem ML logs
Write-Host "Watching ML Train Init..." -ForegroundColor Cyan
kubectl logs -n spark -l job-name=ml-train-init --tail=100 -f

# Verify CronJobs đã được tạo
kubectl get cronjob -n spark
# OUTPUT:
# NAME                SCHEDULE      SUSPEND   ACTIVE   LAST SCHEDULE
# batch-pipeline      */10 * * * *  False     0        2m
# house-price-train   0 * * * *     False     0        5m
```

**Giải thích:**
- ✅ `batch-pipeline-init` chạy ngay (Bronze → Silver → Gold)
- ✅ `ml-train-init` chạy ngay (Train → Inference)
- ⏰ `batch-pipeline` CronJob tự động chạy mỗi 10 phút
- ⏰ `house-price-train` CronJob tự động chạy mỗi giờ

---

## 📡 BƯỚC 5: Chạy Kafka Producer & Consumer (Manual)

### MỞ 3 TERMINAL POWERSHELL MỚI:

---

### **TERMINAL 1: PRODUCER (Data Source)**

```powershell
cd D:\2025.1\bigdata\btl_bigdata
$POD_PRO = kubectl get pods -n kafka -l app=producer -o jsonpath='{.items[0].metadata.name}'
kubectl exec -it -n kafka $POD_PRO -- python /app/producer.py
```

**Output mong đợi:**
```
✓ Gửi thành công tới data-stream [0] @ offset 0
✓ Gửi thành công tới data-stream [0] @ offset 1
...
```

**ĐỂ CHẠY 1-2 PHÚT** (gửi ~300-500 messages)

---

### **TERMINAL 2: CONSUMER (Batch Pipeline → Bronze)**

```powershell
cd D:\2025.1\bigdata\btl_bigdata
$POD_CON = kubectl get pods -n kafka -l app=consumer -o jsonpath='{.items[0].metadata.name}'
kubectl exec -it -n kafka $POD_CON -- python /app/consumer.py
```

**Output mong đợi:**
```
✅ [BRONZE] Uploaded 200 records -> s3://house-lake/bronze/dt=2026-01-16/...
✅ [BRONZE] Uploaded 200 records -> s3://house-lake/bronze/dt=2026-01-16/...
```

**ĐỂ CHẠY cho đến khi thấy ít nhất 2-3 batch uploaded**

---

### **TERMINAL 3: SPARK STREAMING (Speed Pipeline → PostgreSQL)**

```powershell
cd D:\2025.1\bigdata\btl_bigdata
$POD_STREAM = kubectl get pods -n kafka -l app=spark-streaming -o jsonpath='{.items[0].metadata.name}'

kubectl exec -it -n kafka $POD_STREAM -- /bin/bash -c "mkdir -p /tmp/ivy2 && /opt/spark/bin/spark-submit --master local[2] --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,org.postgresql:postgresql:42.7.1 --conf spark.jars.ivy=/tmp/ivy2 /app/stream.py"
```

**Output mong đợi:**
```
🚀 Khởi động Spark Streaming...
   Kafka: kafka-service.kafka.svc.cluster.local:9092
   PostgreSQL: jdbc:postgresql://postgres.postgres...
[Batch 0] Đã ghi 15 records vào PostgreSQL
[Batch 1] Đã ghi 23 records vào PostgreSQL
...
```

**ĐỂ CHẠY liên tục** - đây là real-time pipeline!

---

## 📊 BƯỚC 6: Verify Data

### 6.1. Check MinIO (Browser)

URL: `http://localhost:9001` (minioadmin/minioadmin)

**Expected folders in `house-lake`:**
- ✅ `bronze/dt=2026-01-16/...` - Raw data từ Kafka
- ✅ `silver/` - Cleaned data
- ✅ `gold/location_stats/`, `gold/year_trend/` - Aggregations
- ✅ `models/house_price/latest/` - ML model (nếu đã train)

---

### 6.2. Check PostgreSQL

```powershell
# Terminal mới - Port-forward
kubectl -n postgres port-forward svc/postgres 5433:5432
```

**Connect DBeaver:**
- Host: `localhost`
- Port: **5433**
- Database: `house_warehouse`
- User/Pass: `postgres` / `postgres`

**Run SQL:**

```sql
-- Speed Layer (Real-time từ Streaming)
SELECT COUNT(*) FROM house_data_speed;
SELECT * FROM house_data_speed ORDER BY created_at DESC LIMIT 10;

-- Batch Layer (từ Silver job)
SELECT COUNT(*) FROM fact_house;
SELECT location, COUNT(*) FROM fact_house GROUP BY location;

-- Gold Layer (Aggregations)
SELECT * FROM gold_location_stats ORDER BY avg_price DESC LIMIT 5;
SELECT * FROM gold_year_trend ORDER BY year_built DESC LIMIT 5;

-- ML Predictions (nếu đã chạy)
SELECT COUNT(*), run_id FROM house_price_predictions GROUP BY run_id;
```

---

## 🛑 BƯỚC 7: Stop & Cleanup

```powershell
# Stop Producer/Consumer/Streaming (Ctrl+C in các Terminal)

# Pause CronJobs (không xóa, chỉ tạm dừng)
kubectl patch cronjob batch-pipeline -n spark -p '{"spec":{"suspend":true}}'
kubectl patch cronjob house-price-train -n spark -p '{"spec":{"suspend":true}}'

# Hoặc xóa hoàn toàn
kubectl delete cronjob batch-pipeline house-price-train -n spark
kubectl delete job batch-pipeline-init ml-train-init -n spark
kubectl delete configmap batch-pipeline-config spark-ml-train-jobs -n spark

# Scale down Kafka pods (optional)
kubectl scale -n kafka deploy/producer-data --replicas=0
kubectl scale -n kafka deploy/consumer-logger --replicas=0
kubectl scale -n kafka deploy/spark-streaming --replicas=0
```

---

## 📈 Architecture Overview

```
┌──────────────────────────────────────────────────┐
│         Producer → Kafka (data-stream)           │
└─────────────┬────────────────────────────────────┘
              │
     ┌────────┴─────────┐
     │                  │
┌────▼──────┐   ┌───────▼────────────────────┐
│  SPEED    │   │  BATCH LAYER (Auto)        │
│  LAYER    │   │                            │
│ (Manual)  │   │  Consumer → Bronze         │
├───────────┤   │      ↓                     │
│ Spark     │   │  [Init Job - chạy ngay]    │
│ Streaming │   │  batch-pipeline-init       │
│    ↓      │   │  - Silver (Clean+Features) │
│ PostgreSQL│   │  - Gold (Aggregations)     │
│ house_    │   │      ↓                     │
│ data_speed│   │  [CronJob - mỗi 10 phút]   │
│           │   │  batch-pipeline            │
│ <10s      │   │      ↓                     │
│           │   │  [Init Job - chạy ngay]    │
│           │   │  ml-train-init             │
│           │   │  - Train + Inference       │
│           │   │      ↓                     │
│           │   │  [CronJob - mỗi giờ]       │
│           │   │  house-price-train         │
└───────────┘   └────────────────────────────┘
      │                    │
      └────────┬───────────┘
               ▼
    ┌──────────────────────┐
    │   PostgreSQL         │
    │  - house_data_speed  │
    │  - fact_house        │
    │  - gold_* (4 tables) │
    │  - ml_metrics        │
    │  - predictions       │
    └──────────────────────┘
```

---

## 🐛 Troubleshooting

### Producer không gửi được message
```powershell
kubectl logs -n kafka -l app=producer --tail=50
# Check Kafka connection
```

### Consumer không ghi được MinIO
```powershell
kubectl logs -n kafka -l app=consumer --tail=50
# Check MinIO bucket tồn tại
```

### Silver job lỗi "Path not found"
```powershell
# Đảm bảo Consumer đã chạy và upload Bronze
# Check MinIO console xem có bronze/ folder
```

### Spark Streaming không ghi PostgreSQL
```powershell
kubectl logs -n kafka -l app=spark-streaming --tail=100
# Check PostgreSQL connection
```

---

## 🎯 Success Criteria

✅ **Init Jobs (Chạy ngay khi deploy):**
- `batch-pipeline-init` hoàn thành: Bronze → Silver → Gold
- `ml-train-init` hoàn thành: Train model → Inference
- PostgreSQL có data trong `fact_house`, `gold_*`, `house_price_predictions`

✅ **CronJobs (Auto schedule):**
- `batch-pipeline` CronJob tạo thành công (schedule: `*/10 * * * *`)
- `house-price-train` CronJob tạo thành công (schedule: `0 * * * *`)
- Jobs tự động chạy theo schedule

✅ **Speed Pipeline (Manual):**
- Spark Streaming ghi data vào `house_data_speed`
- Real-time latency < 10s

---

## 📝 Notes

- **Init Jobs:** Chạy 1 lần khi deploy, không retry tự động nếu fail
- **CronJobs:** Tự động chạy theo schedule, có retry nếu fail
- **Dedup strategy:** `pg-max-offset` - chỉ process offset mới từ Kafka
- **Manual trigger:** `kubectl create job --from=cronjob/batch-pipeline manual-$(date +%s) -n spark`
- **Performance:** Batch ~1-2 phút, ML ~5-10 phút

**Happy Data Engineering! 🚀**
