# BigData Pipeline (Kafka → MinIO → Spark → Postgres → ML)

Pipeline:

- Kafka topic `data-stream`
- Consumer ghi Bronze lên MinIO bucket `house-lake` (prefix `bronze/`)
- Spark chạy Silver/Gold đọc-ghi MinIO qua S3A (prefix `silver/`, `gold/`)
- Postgres làm “serving layer” để xem bằng DBeaver
- ML train + inference (CronJob) ghi model/predictions lên MinIO + Postgres

## 0) Yêu cầu

- Windows + PowerShell
- `minikube`, `kubectl`
- DBeaver (tuỳ chọn)

Ghi chú:

- Nếu `kubectl config current-context` đã trỏ vào Minikube thì dùng `kubectl` trực tiếp.
- Nếu chưa, bạn có thể thay mọi `kubectl ...` bằng `minikube kubectl -- ...`.

## 1) Start cluster

```powershell
minikube start --driver=docker
kubectl get nodes
```

## 2) Deploy hạ tầng (MinIO + Postgres + Kafka + Spark runner)

### 2.1 MinIO

```powershell
kubectl create namespace minio>$null
kubectl apply -n minio -f minio/config_minio.yaml
kubectl -n minio get pods -w
```

### 2.2 PostgreSQL

```powershell
kubectl apply -f postgres/postgres.yaml
kubectl -n postgres get pods -w
```

### 2.3 Kafka broker

```powershell
kubectl create namespace kafka>$null
kubectl apply -n kafka -f kafka/kafka.yaml
kubectl -n kafka get pods -w
```

### 2.4 Spark runner (pod để chạy `spark-submit` trong cluster)

```powershell
kubectl create namespace spark>$null
kubectl apply -f spark/spark-runner.k8s.yaml
kubectl -n spark get pods -w
```

## 3) Kafka → Bronze (MinIO)

### 3.1 Deploy producer/consumer (dev mode)

```powershell
kubectl apply -n kafka -f kafka/flow.yaml
kubectl -n kafka get pods -w
```

### 3.2 Copy code vào pods

Chạy từ thư mục root `BigData`:

```powershell
$POD_CON = (kubectl get pods -n kafka -l app=consumer -o jsonpath='{.items[0].metadata.name}')
$POD_PRO = (kubectl get pods -n kafka -l app=producer -o jsonpath='{.items[0].metadata.name}')

kubectl cp .\kafka\consumer.py -n kafka "$POD_CON`:/app/consumer.py"
kubectl cp .\kafka\upload_to_storage.py -n kafka "$POD_CON`:/app/upload_to_storage.py"

kubectl cp .\kafka\producer.py -n kafka "$POD_PRO`:/app/producer.py"
kubectl cp .\kafka\house_data.json -n kafka "$POD_PRO`:/app/house_data.json"
```

Nếu bạn đang đứng sẵn trong thư mục `BigData\kafka` thì bỏ prefix `kafka\` (vd `kubectl cp .\consumer.py ...`).

### 3.3 Chạy consumer + producer

Terminal 1:

```powershell
kubectl -n kafka exec -it deploy/consumer-logger -- python /app/consumer.py
```

Terminal 2:

```powershell
kubectl -n kafka exec -it deploy/producer-data -- python /app/producer.py
```

Dừng bằng `Ctrl + C`. (Bạn có thể dừng producer trước, consumer sau.)

## 4) Spark Silver (Bronze → Silver) + tuỳ chọn ghi Postgres

Spark jobs dùng:

- S3A packages: `org.apache.hadoop:hadoop-aws:3.3.4`, `com.amazonaws:aws-java-sdk-bundle:1.12.262`
- JDBC package (khi ghi Postgres): `org.postgresql:postgresql:42.7.4`

### 4.1 Copy Spark jobs vào `spark-runner`

```powershell
kubectl -n spark exec spark-runner -- /bin/sh -lc "mkdir -p /opt/project/jobs"
kubectl cp .\spark\jobs\common.py -n spark "spark-runner:/opt/project/jobs/common.py"
kubectl cp .\spark\jobs\silver_job.py -n spark "spark-runner:/opt/project/jobs/silver_job.py"
kubectl cp .\spark\jobs\gold_job.py -n spark "spark-runner:/opt/project/jobs/gold_job.py"
```

Lưu ý: Nếu bạn vừa sửa code trên máy local, hãy `kubectl cp` lại vào pod trước khi chạy.

### 4.2 Run Silver (ghi MinIO `silver/`)

```powershell
kubectl -n spark exec spark-runner -- sh -c "MINIO_ENDPOINT=http://minio.minio.svc.cluster.local:9000 MINIO_ACCESS_KEY=minioadmin MINIO_SECRET_KEY=minioadmin /opt/spark/bin/spark-submit --packages org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262 /opt/project/jobs/silver_job.py --bucket house-lake --input-format json"
```

Lưu ý: Silver ghi vào MinIO bằng mode `append`. Nếu bạn rerun nhiều lần mà không xoá `silver/` thì dữ liệu Silver sẽ bị trùng.

### 4.3 (Tuỳ chọn) Run Silver và ghi thêm vào Postgres để visualize (bảng `fact_house`)

```powershell
kubectl -n spark exec spark-runner -- sh -c "MINIO_ENDPOINT=http://minio.minio.svc.cluster.local:9000 MINIO_ACCESS_KEY=minioadmin MINIO_SECRET_KEY=minioadmin /opt/spark/bin/spark-submit --packages org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262,org.postgresql:postgresql:42.7.4 /opt/project/jobs/silver_job.py --bucket house-lake --input-format json --write-postgres --pg-url jdbc:postgresql://postgres.postgres.svc.cluster.local:5432/house_warehouse --pg-user postgres --pg-password postgres --pg-table fact_house"
```

Mặc định `fact_house` ghi mode `append`.

- Tránh xử lý lại Bronze khi rerun: giữ `--write-postgres` và dùng `--dedup-strategy pg-max-offset` (mặc định). Job sẽ đọc `MAX(offset)` theo `(topic, partition)` từ `fact_house` và chỉ xử lý record mới.
- Backfill/reset (làm lại từ đầu): xoá prefix `silver/` trên MinIO và chạy lại với `--pg-mode overwrite` (hoặc drop table `fact_house`), sau đó mới rerun các bước Gold/ML.

## 5) Spark Gold (Silver → Gold + Postgres)

Gold hiện tạo 2 output:

- MinIO: `gold/location_stats/`, `gold/year_trend/`
- Postgres: `gold_location_stats`, `gold_year_trend` (khi bật `--write-postgres`)

```powershell
kubectl -n spark exec spark-runner -- sh -c "MINIO_ENDPOINT=http://minio.minio.svc.cluster.local:9000 MINIO_ACCESS_KEY=minioadmin MINIO_SECRET_KEY=minioadmin /opt/spark/bin/spark-submit --packages org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262,org.postgresql:postgresql:42.7.4 /opt/project/jobs/gold_job.py --bucket house-lake --write-postgres"
```

Lưu ý: `gold_job.py` hiện không có flag `--pg-table` (vì nó ghi 2 bảng cố định).

## 6) Train model + inference

### 6.1 Train (K8S Job)

```powershell
kubectl -n spark delete job house-price-train --ignore-not-found=true
kubectl apply -f spark/house-price-train-job.yaml
kubectl -n spark wait --for=condition=complete job/house-price-train --timeout=900s
kubectl -n spark logs job/house-price-train --tail=120
```

Kết quả mong đợi:

- MinIO: `models/house_price/latest/` và `models/house_price/runs/<run_id>/`
- Postgres: bảng `ml_house_price_model_metrics` có thêm 1 row

### 6.2 Inference (CronJob theo lịch)

```powershell
kubectl apply -f spark/house-price-inference-cronjob.yaml
kubectl -n spark get cronjob
```

Chạy thử ngay (manual job):

```powershell
kubectl -n spark delete job house-price-inference-manual --ignore-not-found=true
kubectl -n spark create job --from=cronjob/house-price-inference house-price-inference-manual
kubectl -n spark wait --for=condition=complete job/house-price-inference-manual --timeout=900s
kubectl -n spark logs job/house-price-inference-manual --tail=160
```

Kết quả mong đợi:

- MinIO: `gold/predictions_house_price/`
- Postgres: bảng `house_price_predictions` tăng row theo thời gian

## 7) Xem dữ liệu (MinIO Console + DBeaver)

### 7.1 MinIO Console

Cách ổn định (port-forward, không đổi port):

```powershell
kubectl -n minio port-forward svc/minio-public 9001:9001
```

Mở: `http://localhost:9001` (user/pass: `minioadmin` / `minioadmin`).

Ghi chú: `minikube service minio-public -n minio` cũng dùng được nhưng nó tạo tunnel; trên Windows (Docker driver) bạn phải giữ terminal mở.

### 7.2 Postgres (DBeaver)

```powershell
kubectl -n postgres port-forward svc/postgres 5432:5432
```

DBeaver:

- Host: `localhost`
- Port: `5432`
- DB: `house_warehouse`
- User/Pass: `postgres` / `postgres`

Tables thường dùng:

- `fact_house` (tuỳ chọn, từ Silver)
- `gold_location_stats`, `gold_year_trend`
- `ml_house_price_model_metrics`
- `house_price_predictions`

## 8) Checklist verify end-to-end

- MinIO bucket `house-lake` có `bronze/`, `silver/`, `gold/`, `models/`
- Postgres có: `gold_location_stats`, `gold_year_trend`, `ml_house_price_model_metrics`, `house_price_predictions` (và `fact_house` nếu bạn bật Silver→Postgres)
- Inference CronJob chạy định kỳ và `house_price_predictions` tăng

## 9) Troubleshooting nhanh

- `kubectl cp` fail trên Windows: kiểm tra bạn đang đứng ở thư mục nào; nếu đã ở `BigData\kafka` thì copy bằng `.\consumer.py` thay vì `.\kafka\consumer.py`.
- Gold không chạy khi bạn truyền thêm flag lạ (vd `--pg-table`): kiểm tra `--help` bằng `kubectl -n spark exec spark-runner -- sh -c "/opt/spark/bin/spark-submit /opt/project/jobs/gold_job.py --help"`.
