# MinIO (S3-compatible) trên Minikube

MinIO dùng làm storage cho Data Lake (bucket `house-lake`).

Ghi chú:

- Nếu `kubectl` chưa trỏ đúng context Minikube, bạn có thể thay `kubectl` bằng `minikube kubectl --`.

## 1) Deploy

Chạy từ thư mục root `BigData`:

```powershell
kubectl create namespace minio 2>$null
kubectl apply -n minio -f minio/config_minio.yaml
kubectl -n minio get pods -w
```

## 2) Truy cập MinIO Console

### Cách ổn định (khuyến nghị): port-forward

```powershell
kubectl -n minio port-forward svc/minio-public 9001:9001
```

Mở: `http://localhost:9001`

### Cách khác: `minikube service`

```powershell
minikube service minio-public -n minio
```

Ghi chú: trên Windows (Minikube Docker driver), `minikube service` có thể tạo tunnel và bạn cần giữ terminal mở.

## 3) Credentials

- Access Key: `minioadmin`
- Secret Key: `minioadmin`

## 4) Internal Service URL

Ứng dụng chạy **trong cluster** (Kafka consumer, Spark jobs, Airflow, ...) truy cập MinIO qua:

```text
http://minio.minio.svc.cluster.local:9000
```

## 5) Bucket & prefixes

Tạo bucket: `house-lake`

Recommended prefixes:

- `bronze/`
- `silver/`
- `gold/`
- `models/`
