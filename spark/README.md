# Spark

README này ưu tiên cách chạy Spark **trong Kubernetes** (Minikube) bằng pod `spark-runner`.

## 0) Tổng quan

- Spark job scripts nằm ở `spark/jobs/`
- Job chạy trong cluster, đọc/ghi MinIO qua S3A (`s3a://...`)
- Khi cần visualize, có thể ghi thêm vào Postgres qua JDBC

Packages dùng:

- S3A: `org.apache.hadoop:hadoop-aws:3.3.4`, `com.amazonaws:aws-java-sdk-bundle:1.12.262`
- JDBC: `org.postgresql:postgresql:42.7.4`

## 1) Deploy `spark-runner`

Chạy từ thư mục root `BigData`:

```powershell
kubectl create namespace spark 2>$null
kubectl apply -f spark/spark-runner.k8s.yaml
kubectl -n spark get pods -w
```

## 2) Copy jobs vào runner

```powershell
kubectl -n spark exec spark-runner -- /bin/sh -lc "mkdir -p /opt/project/jobs"

kubectl cp .\spark\jobs\common.py -n spark "spark-runner:/opt/project/jobs/common.py"
kubectl cp .\spark\jobs\silver_job.py -n spark "spark-runner:/opt/project/jobs/silver_job.py"
kubectl cp .\spark\jobs\gold_job.py -n spark "spark-runner:/opt/project/jobs/gold_job.py"
```

Ghi chú quan trọng: nếu bạn sửa job code trên máy local, cần `kubectl cp` lại vào pod, nếu không pod sẽ chạy bản cũ.

## 3) Run Silver (Bronze → Silver)

```powershell
kubectl -n spark exec spark-runner -- sh -c "MINIO_ENDPOINT=http://minio.minio.svc.cluster.local:9000 MINIO_ACCESS_KEY=minioadmin MINIO_SECRET_KEY=minioadmin /opt/spark/bin/spark-submit --packages org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262 /opt/project/jobs/silver_job.py --bucket house-lake --input-format json"
```

Lưu ý: Silver ghi vào MinIO bằng mode `append`. Nếu cần rerun sạch (không trùng dữ liệu), hãy xoá prefix `silver/` trên MinIO trước.

### 3.1 (Optional) Silver → Postgres (table `fact_house`)

```powershell
kubectl -n spark exec spark-runner -- sh -c "MINIO_ENDPOINT=http://minio.minio.svc.cluster.local:9000 MINIO_ACCESS_KEY=minioadmin MINIO_SECRET_KEY=minioadmin /opt/spark/bin/spark-submit --packages org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262,org.postgresql:postgresql:42.7.4 /opt/project/jobs/silver_job.py --bucket house-lake --input-format json --write-postgres --pg-url jdbc:postgresql://postgres.postgres.svc.cluster.local:5432/house_warehouse --pg-user postgres --pg-password postgres --pg-table fact_house --pg-mode append --dedup-strategy pg-max-offset"
```

Ghi chú:

- Mặc định job sẽ dedup theo Kafka offsets dựa trên state trong Postgres (`pg-max-offset`) để tránh xử lý lại Bronze.
- Backfill/reset: drop table `fact_house` (hoặc chạy `--pg-mode overwrite`) và xoá `silver/` nếu bạn muốn làm lại Silver từ đầu.

## 4) Run Gold (Silver → Gold) + Postgres

```powershell
kubectl -n spark exec spark-runner -- sh -c "MINIO_ENDPOINT=http://minio.minio.svc.cluster.local:9000 MINIO_ACCESS_KEY=minioadmin MINIO_SECRET_KEY=minioadmin /opt/spark/bin/spark-submit --packages org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262,org.postgresql:postgresql:42.7.4 /opt/project/jobs/gold_job.py --bucket house-lake --write-postgres"
```

Lưu ý: `gold_job.py` ghi 2 bảng cố định (`gold_location_stats`, `gold_year_trend`) và **không** support `--pg-table`.

## 5) (Legacy) Local Spark bằng docker-compose

Nếu bạn muốn chạy thử local Spark bằng Docker (không dùng K8S), file compose nằm ở `spark/docker-compose.spark.yaml`.

```bash
docker compose -f spark/docker-compose.spark.yaml up -d
docker compose -f spark/docker-compose.spark.yaml down
```
