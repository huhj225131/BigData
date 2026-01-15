# 🚀 QUICK START - Chạy Song Song 2 Pipeline

Lambda Architecture: **Batch Layer + Speed Layer** chạy đồng thời

---

## 📋 Prerequisites

- ✅ Minikube running (`minikube start`)
- ✅ kubectl configured
- ✅ Đang ở thư mục root: `D:\2025.1\bigdata\btl_bigdata`

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

# 1.4. Spark Runner
kubectl create namespace spark
kubectl apply -f spark/spark-runner.k8s.yaml
kubectl wait --for=condition=Ready pod/spark-runner -n spark --timeout=90s

# 1.5. Kafka Flow (Producer + Consumer + Spark Streaming)
kubectl apply -f kafka/flow.yaml
kubectl apply -f spark_streaming/spark-deployment.yaml
Start-Sleep -Seconds 30

# Verify
kubectl get pods -A | Select-String -Pattern "(kafka|minio|postgres|spark)"
```

**Expected: Tất cả pods `Running`**

---

## 🔧 BƯỚC 2: Copy Code vào Pods

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

# 2.4. Spark Batch Jobs
kubectl exec -n spark spark-runner -- mkdir -p /opt/project/jobs
kubectl cp spark/jobs/common.py -n spark spark-runner:/opt/project/jobs/
kubectl cp spark/jobs/silver_job.py -n spark spark-runner:/opt/project/jobs/
kubectl cp spark/jobs/gold_job.py -n spark spark-runner:/opt/project/jobs/
kubectl cp spark/jobs/ml_train_house_price.py -n spark spark-runner:/opt/project/jobs/

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

## 🚀 BƯỚC 4: Chạy Song Song 2 Pipeline

### MỞ 4 TERMINAL POWERSHELL MỚI:

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

### **TERMINAL 4: BATCH JOBS (Bronze → Silver → Gold)**

```powershell
cd D:\2025.1\bigdata\btl_bigdata

# Đợi Producer/Consumer chạy 30 giây để có Bronze data
Write-Host "Đợi Bronze data..." -ForegroundColor Yellow
Start-Sleep -Seconds 30

# 1. Silver Job (Bronze → Silver)
Write-Host "`n[1/2] Running Silver Job..." -ForegroundColor Cyan
kubectl exec -n spark spark-runner -- sh -c "MINIO_ENDPOINT=http://minio.minio.svc.cluster.local:9000 MINIO_ACCESS_KEY=minioadmin MINIO_SECRET_KEY=minioadmin /opt/spark/bin/spark-submit --packages org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262,org.postgresql:postgresql:42.7.1 /opt/project/jobs/silver_job.py --bucket house-lake --input-format json --write-postgres"

Write-Host "✅ Silver completed!" -ForegroundColor Green

# 2. Gold Job (Silver → Gold)
Write-Host "`n[2/2] Running Gold Job..." -ForegroundColor Cyan
kubectl exec -n spark spark-runner -- sh -c "MINIO_ENDPOINT=http://minio.minio.svc.cluster.local:9000 MINIO_ACCESS_KEY=minioadmin MINIO_SECRET_KEY=minioadmin /opt/spark/bin/spark-submit --packages org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262,org.postgresql:postgresql:42.7.1 /opt/project/jobs/gold_job.py --bucket house-lake --write-postgres"

Write-Host "✅ Gold completed!" -ForegroundColor Green
Write-Host "`n🎉 Batch pipeline finished!" -ForegroundColor Magenta
```

---

## 🤖 BƯỚC 5: ML Pipeline (Optional)

```powershell
# 5.1. Train Model
kubectl apply -f spark/house-price-train-job.yaml
kubectl wait --for=condition=complete -n spark job/house-price-train --timeout=600s
kubectl logs -n spark job/house-price-train --tail=50

# 5.2. Start Inference CronJob
kubectl apply -f spark/house-price-inference-cronjob.yaml

# 5.3. Trigger manual inference
kubectl create job -n spark house-price-inference-manual --from=cronjob/house-price-inference
kubectl wait --for=condition=complete -n spark job/house-price-inference-manual --timeout=600s
kubectl logs -n spark job/house-price-inference-manual --tail=50
```

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

## 🛑 BƯỚC 7: Stop Pipelines

```powershell
# Stop Producer (Ctrl+C in Terminal 1)
# Stop Consumer (Ctrl+C in Terminal 2)  
# Stop Streaming (Ctrl+C in Terminal 3)

# Stop ML CronJob
kubectl delete cronjob -n spark house-price-inference

# Scale down (optional)
kubectl scale -n kafka deploy/producer-data --replicas=0
kubectl scale -n kafka deploy/consumer-logger --replicas=0
kubectl scale -n kafka deploy/spark-streaming --replicas=0
```

---

## 📈 Architecture Overview

```
┌─────────────────────────────────────────────────────────┐
│                   DATA SOURCE                           │
│              Producer → Kafka (data-stream)             │
└────────────────────┬────────────────────────────────────┘
                     │
        ┌────────────┴────────────┐
        │                          │
┌───────▼────────┐      ┌─────────▼──────────┐
│  SPEED LAYER   │      │  BATCH LAYER       │
│  (Real-time)   │      │  (High Accuracy)   │
├────────────────┤      ├────────────────────┤
│ Kafka          │      │ Consumer           │
│   ↓            │      │   ↓                │
│ Spark Stream   │      │ Bronze (MinIO)     │
│   ↓            │      │   ↓                │
│ PostgreSQL     │      │ Silver Job         │
│ house_data_    │      │   ↓                │
│ speed          │      │ Silver (MinIO+PG)  │
│                │      │   ↓                │
│ Latency: <10s  │      │ Gold Job           │
│                │      │   ↓                │
│                │      │ Gold (MinIO+PG)    │
│                │      │   ↓                │
│                │      │ ML Train           │
│                │      │   ↓                │
│                │      │ ML Inference       │
│                │      │                    │
│                │      │ Latency: hours     │
└────────────────┘      └────────────────────┘
         │                       │
         └───────────┬───────────┘
                     ▼
         ┌───────────────────────┐
         │   SERVING LAYER       │
         │   PostgreSQL          │
         │   - house_data_speed  │
         │   - fact_house        │
         │   - gold_*            │
         │   - predictions       │
         └───────────────────────┘
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

✅ **Speed Pipeline (Real-time):**
- Producer gửi messages vào Kafka
- Spark Streaming đọc và ghi PostgreSQL
- `house_data_speed` table tăng real-time
- Latency: < 10 seconds

✅ **Batch Pipeline (Accuracy):**
- Consumer ghi Bronze vào MinIO
- Silver job tạo cleaned data
- Gold job tạo aggregations
- PostgreSQL có `fact_house` và `gold_*` tables
- Latency: minutes

✅ **ML Pipeline:**
- Model trained và saved vào MinIO
- Predictions generated và saved
- PostgreSQL có `house_price_predictions`

---

## 📝 Notes

- **First run:** Bronze → Silver → Gold → ML Train → Inference
- **Incremental runs:** Chỉ chạy Silver (sẽ process record mới), sau đó Gold, Inference
- **Full refresh:** Delete MinIO folders và PostgreSQL tables, chạy lại từ đầu
- **Performance:** Producer rate ~100-200 msg/batch, Consumer batch size 200

**Happy Data Engineering! 🚀**
