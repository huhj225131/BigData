# Big Data Pipeline - Deployment & Schedule

## 🚀 QUICK START

### **Deploy toàn bộ pipeline (Chạy ngay + Auto schedule)**

```bash
# 1. Apply cả 2 file (Job sẽ chạy ngay lập tức)
kubectl apply -f spark/batch-pipeline-cronjob.yaml
kubectl apply -f spark/house-price-train-job.yaml

# 2. Xem job init đang chạy
kubectl get jobs -n spark
# OUTPUT:
# batch-pipeline-init   0/1   10s
# ml-train-init         0/1   8s

# 3. Xem logs real-time
kubectl logs -n spark -l job-name=batch-pipeline-init --tail=100 -f
kubectl logs -n spark -l job-name=ml-train-init --tail=100 -f

# 4. Verify CronJob đã được tạo
kubectl get cronjob -n spark
```

---

## 📅 CẤU HÌNH SCHEDULE

### **1. Batch Pipeline (Bronze → Silver → Gold)**

**File:** [batch-pipeline-cronjob.yaml](batch-pipeline-cronjob.yaml)

**Chứa 2 resources:**
- ✅ **Job `batch-pipeline-init`**: Chạy **NGAY** khi apply (1 lần duy nhất)
- ⏰ **CronJob `batch-pipeline`**: Tự động chạy **mỗi 10 phút** (`*/10 * * * *`)

**Workflow (2 bước tuần tự):**
1. **Bronze → Silver:** Clean + Feature Engineering
   - 4 features: `price_per_sqft`, `house_age`, `total_rooms`, `condition_score`
   - Incremental processing: `pg-max-offset` dedup (chỉ xử lý offset mới)
   - Output: MinIO `silver/` + PostgreSQL `fact_house`

2. **Silver → Gold:** Aggregations
   - 4 bảng: `gold_location_stats`, `gold_condition_stats`, `gold_bedroom_analysis`, `gold_year_built_trends`
   - Output: MinIO `gold/` + PostgreSQL `gold_*` tables

**Thời gian chạy:** ~1-2 phút/lần

---

### **2. ML Training + Inference**

**File:** [house-price-train-job.yaml](house-price-train-job.yaml)

**Chứa 2 resources:**
- ✅ **Job `ml-train-init`**: Chạy **NGAY** khi apply (1 lần duy nhất)
- ⏰ **CronJob `house-price-train`**: Tự động chạy **mỗi giờ** (`0 * * * *`)

**Workflow (2 bước tuần tự):**
1. **Train model** từ Silver data
   - Features: 4 original + 4 engineered + 2 categorical (OHE)
   - Model: Random Forest (50 trees, max depth 10)
   - Metrics: RMSE, R²
   - Output: MinIO `models/house_price/latest` + PostgreSQL `ml_house_price_model_metrics`

2. **Inference** trên toàn bộ Silver data
   - Predict giá cho tất cả houses
   - Output: MinIO `gold/predictions_house_price/` + PostgreSQL `house_price_predictions`

**Thời gian chạy:** ~5-10 phút/lần

---

## ⏰ TIMELINE DEPLOY

```
00:00:00 - kubectl apply (cả 2 file)
00:00:02 - batch-pipeline-init bắt đầu (Job init)
00:00:03 - ml-train-init bắt đầu (Job init)
00:02:30 - batch-pipeline-init hoàn thành ✓
00:08:45 - ml-train-init hoàn thành ✓
00:10:00 - batch-pipeline CronJob chạy lần 1 (auto)
00:20:00 - batch-pipeline CronJob chạy lần 2 (auto)
01:00:00 - house-price-train CronJob chạy lần 1 (auto)
...
```

**Tần suất chạy mỗi ngày:**
- Batch Pipeline: **144 lần** (mỗi 10 phút)
- ML Train+Inference: **24 lần** (mỗi giờ)

---

## 🔧 OPERATIONS

### **Xem logs**

```bash
# Logs của Job init (chạy 1 lần)
kubectl logs -n spark -l job-name=batch-pipeline-init --tail=100
kubectl logs -n spark -l job-name=ml-train-init --tail=100

# Logs của CronJob (chạy định kỳ)
kubectl logs -n spark -l job-name=batch-pipeline-28435440 --tail=100 -f
kubectl logs -n spark -l job-name=house-price-train-28435400 --tail=100 -f
```

### **Trigger thêm lần nữa (manual)**

```bash
# Chạy batch thêm 1 lần (không đợi schedule)
kubectl create job --from=cronjob/batch-pipeline batch-manual-$(date +%s) -n spark

# Chạy ML train thêm 1 lần
kubectl create job --from=cronjob/house-price-train train-manual-$(date +%s) -n spark
```

### **Tạm dừng CronJob**

```bash
# Suspend CronJob (không chạy tự động nữa)
kubectl patch cronjob batch-pipeline -n spark -p '{"spec":{"suspend":true}}'
kubectl patch cronjob house-price-train -n spark -p '{"spec":{"suspend":true}}'

# Resume lại
kubectl patch cronjob batch-pipeline -n spark -p '{"spec":{"suspend":false}}'
kubectl patch cronjob house-price-train -n spark -p '{"spec":{"suspend":false}}'
```

### **Xóa toàn bộ**

```bash
# Xóa tất cả jobs và cronjobs
kubectl delete job batch-pipeline-init -n spark
kubectl delete job ml-train-init -n spark
kubectl delete cronjob batch-pipeline -n spark
kubectl delete cronjob house-price-train -n spark
kubectl delete configmap batch-pipeline-config -n spark
kubectl delete configmap spark-ml-train-jobs -n spark
```

---

## 🎯 KIẾN TRÚC PIPELINE

```
┌─────────────────────────────────────────────────────────────┐
│                    KAFKA PRODUCER                            │
│                   (house_data.json)                          │
└─────────────────────┬───────────────────────────────────────┘
                      │ Topic: data-stream
                      ▼
┌─────────────────────────────────────────────────────────────┐
│                   KAFKA CONSUMER                             │
│         Batch 200 msgs hoặc 3 phút → Bronze (JSONL)         │
└─────────────────────┬───────────────────────────────────────┘
                      │
                      ▼
┌─────────────────────────────────────────────────────────────┐
│                    MinIO: bronze/                            │
│      dt=YYYY-MM-DD/hour=HH/topic=*/partition=*/*.jsonl      │
└─────────────────────┬───────────────────────────────────────┘
                      │
                      │ [INIT: Chạy ngay khi deploy]
                      │ [AUTO: Mỗi 10 phút]
                      ▼
         ┌────────────────────────────────────┐
         │   BATCH PIPELINE (Spark Job)       │
         │                                     │
         │  Step 1: Bronze → Silver            │
         │  - Clean data                       │
         │  - Feature Engineering (4 features) │
         │  - Dedup (pg-max-offset)            │
         │                                     │
         │  Step 2: Silver → Gold              │
         │  - 4 Aggregation Tables             │
         └────────────┬───────┬────────────────┘
                      │       │
         ┌────────────▼───┐   └───────────┐
         │  MinIO Silver/ │               │
         │  (Parquet)     │               │
         └────────┬───────┘               │
                  │                       │
                  │ [INIT: Chạy ngay]    │
                  │ [AUTO: Mỗi giờ]     │
                  ▼                       ▼
    ┌─────────────────────────┐   ┌──────────────────┐
    │  ML TRAIN + INFERENCE   │   │  MinIO Gold/     │
    │  - Random Forest        │   │  - location_*    │
    │  - RMSE, R² metrics     │   │  - condition_*   │
    │  - Predict all houses   │   │  - bedroom_*     │
    └──────────┬──────────────┘   │  - year_trends_* │
               │                   └────────┬─────────┘
               │                            │
               ▼                            ▼
    ┌────────────────────┐      ┌─────────────────────┐
    │ MinIO models/      │      │   PostgreSQL        │
    │ - latest/          │      │  - fact_house       │
    │ - runs/{run_id}/   │      │  - gold_* (4 bảng)  │
    └────────────────────┘      │  - ml_metrics       │
                                │  - predictions      │
                                └──────────┬──────────┘
                                           │
                                           ▼
                                ┌─────────────────────┐
                                │   DASHBOARD         │
                                │   (Streamlit)       │
                                └─────────────────────┘
```

---

## 📋 DATABASE SCHEMA (PostgreSQL)

### **Layer: Batch (from Spark)**

| Bảng | Chế độ | Nguồn | Mô tả |
|------|--------|-------|-------|
| `fact_house` | append | Silver job | Clean house data (có topic, partition, offset để dedup) |

### **Layer: Gold (Aggregations)**

| Bảng | Chế độ | Nguồn | Mô tả |
|------|--------|-------|-------|
| `gold_location_stats` | overwrite | Gold job | Thống kê theo location (avg_price, median, total_houses...) |
| `gold_condition_stats` | overwrite | Gold job | Thống kê theo condition (Excellent, Good, Fair) |
| `gold_bedroom_analysis` | overwrite | Gold job | Phân tích theo số bedrooms |
| `gold_year_built_trends` | overwrite | Gold job | Xu hướng giá theo decade (1980s, 1990s...) |

### **Layer: ML**

| Bảng | Chế độ | Nguồn | Mô tả |
|------|--------|-------|-------|
| `ml_house_price_model_metrics` | append | ML Train | RMSE, R², run_id, model_path |
| `house_price_predictions` | append | ML Inference | actual_price vs predicted_price |

### **Layer: Speed (from Spark Streaming)**

| Bảng | Chế độ | Nguồn | Mô tả |
|------|--------|-------|-------|
| `house_data_speed` | append | Spark Streaming | Real-time data (5s latency) |

---

## ⚙️ RESOURCE REQUIREMENTS

### **Batch Pipeline**
- Memory: 2-4 GiB
- CPU: 1-2 cores
- Thời gian: ~1-2 phút
- Spark mode: `local[*]`

### **ML Train + Inference**
- Memory: ~4 GiB (với packages download)
- CPU: 2+ cores
- Thời gian: ~5-10 phút
- Dependencies: hadoop-aws, aws-java-sdk, postgresql, numpy

---

## 🔍 TROUBLESHOOTING

### **Job init không chạy hoặc failed**

```bash
# Kiểm tra job status
kubectl get jobs -n spark

# Xem lỗi
kubectl describe job batch-pipeline-init -n spark
kubectl describe job ml-train-init -n spark

# Xem logs
kubectl logs -n spark -l job-name=batch-pipeline-init --tail=200
kubectl logs -n spark -l job-name=ml-train-init --tail=200
```

**Lỗi thường gặp:**
- ❌ Bronze folder empty → Chưa có data từ Kafka Consumer
- ❌ PostgreSQL connection refused → Postgres chưa ready hoặc sai password
- ❌ MinIO 403 Forbidden → Sai access key/secret key
- ❌ OutOfMemory → Tăng `driver-memory` và `executor-memory`

### **CronJob không chạy đúng giờ**

```bash
# Kiểm tra schedule
kubectl get cronjob batch-pipeline -n spark -o yaml | grep schedule

# Xem lần chạy cuối
kubectl get cronjob -n spark
# OUTPUT: LAST SCHEDULE column

# Xem history
kubectl get jobs -n spark --sort-by=.metadata.creationTimestamp
```

### **Data bị duplicate**

**Nguyên nhân:** Dedup strategy không hoạt động

**Giải pháp:**
```bash
# Kiểm tra bảng fact_house có topic, partition, offset không
kubectl exec -it postgres-0 -n postgres -- psql -U postgres -d house_warehouse -c "SELECT topic, partition, MAX(offset) FROM fact_house GROUP BY topic, partition;"

# Nếu không có → Lần đầu chạy cần có dữ liệu
# Nếu có → Kiểm tra pg-max-offset strategy trong logs
```

---

## 📝 NOTES

- **Init Jobs** chạy 1 lần khi deploy, không tự động retry nếu fail (phải deploy lại)
- **CronJobs** có `concurrencyPolicy: Forbid` → Không cho 2 job chạy cùng lúc
- **Dedup strategy** `pg-max-offset` cần PostgreSQL có dữ liệu, lần đầu sẽ process all
- **ML model** ghi đè `latest/` mỗi lần train, nhưng archive vào `runs/{run_id}/`
- **Gold tables** dùng `overwrite` mode → Recalculate toàn bộ mỗi lần chạy

### **Thay đổi schedule ML training**
Edit [house-price-train-job.yaml](house-price-train-job.yaml):

```yaml
spec:
  schedule: "0 * * * *"  # Mỗi giờ
  # schedule: "0 */2 * * *"  # Mỗi 2 giờ
  # schedule: "0 0 * * *"  # Mỗi ngày 00:00
  # schedule: "*/30 * * * *"  # Mỗi 30 phút
```

### **Thay đổi ML hyperparameters**
Sửa env trong YAML:

```yaml
env:
  - name: RF_NUM_TREES
    value: "100"  # Default: 50
  - name: RF_MAX_DEPTH
    value: "15"   # Default: 10
  - name: ML_TRAIN_RATIO
    value: "0.85"  # Default: 0.8
```

---

## 🚀 DEPLOYMENT STEPS

### **Lần đầu deploy:**
```bash
# 1. Deploy ML CronJob
kubectl apply -f spark/house-price-train-job.yaml

# 2. Verify
kubectl get cronjob -n spark
kubectl get pods -n spark

# 3. (Optional) Trigger manual job
kubectl create job --from=cronjob/house-price-train house-price-train-first -n spark

# 4. Monitor
kubectl logs -f -n spark -l job-name=house-price-train-first
```

### **Update code:**
```bash
# Edit local files
vim spark/jobs/ml_train_house_price.py

# Re-apply ConfigMap + CronJob
kubectl apply -f spark/house-price-train-job.yaml

# Next scheduled run sẽ dùng code mới
```

---

## 📊 MONITORING

### **Check schedule**
```bash
kubectl get cronjob -n spark
```

### **Xem job history**
```bash
kubectl get jobs -n spark --sort-by=.metadata.creationTimestamp
```

### **Xem predictions trong PostgreSQL**
```sql
-- Latest predictions
SELECT run_id, COUNT(*) as total, AVG(ABS(actual_price - predicted_price)) as mae
FROM house_price_predictions
GROUP BY run_id
ORDER BY run_id DESC
LIMIT 10;

-- Model metrics
SELECT run_id, rmse, r2, as_of_utc
FROM ml_house_price_model_metrics
ORDER BY as_of_utc DESC
LIMIT 10;
```

---

## ✅ SUMMARY
 Files |
|-----------|-----------|--------|-------|-------|
| **Stream → Bronze** | Real-time | Kafka Consumer | ✅ Auto | Bronze NDJSON |
| **Bronze → Silver → Gold** | 10 phút 1 lần | CronJob | ✅ Auto | batch-pipeline-cronjob.yaml |
| **ML Train + Inference** | 1 giờ 1 lần | CronJob | ✅ Auto | house-price-train-job.yam Auto |
| **Silver → Gold** | On-demand | Manual Job | ❌ Manual |

**Lợi ích:**
- ✅ Data pipeline gần real-time (Bronze→Silver mỗi 10 phút)
- ✅ Pipeline tự động hoàn toàn: Bronze → Silver → Gold (mỗi 10 phút)
- ✅ ML train sử dụng features từ Silver (consistency giữa train/inference)
- ✅ Features computed once ở Silver layer, reuse ở ML và Gold
- ✅ Batch window tránh xử lý lại toàn bộ data
- ✅ Inference tự động sau train → Predictions luôn fresh
🎉 **Pipeline đã sẵn sàng production!**
