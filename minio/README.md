
# Triển khai MinIO trên Minikube
Triển khai MinIO Storage để lưu trữ dữ liệu thô
## 1. Khởi tạo Namespace
Để quản lý tài nguyên tách biệt, chúng ta tạo một Namespace riêng cho MinIO:

```powershell
# Tạo namespace chuyên biệt cho hệ thống lưu trữ
minikube kubectl -- create namespace minio
```

## 2. Khởi tạo Namespace
Sau bước này MinIO storage sẽ được khởi tạo thành côngcông
```powershell
# Triển khai StatefulSet và Service cho MinIO
minikube kubectl -- apply -f minio-dev.yaml -n minio

# Kiểm tra trạng thái đến khi Pod báo 'Running'
minikube kubectl -- get pods -n minio
```

## 3. Truy cập giao diện quản trị (Export Port)
```powershell
minikube service minio-public -n minio
```
## 4. Thông tin đăng nhập
###  Thông tin tài khoản
- **Access Key**: `minioadmin`
- **Secret Key**: `minioadmin`
---

###  Địa chỉ kết nối nội bộ (Internal Service URL)

Các ứng dụng chạy **bên trong Kubernetes Cluster** (ví dụ: Consumer, Spark, Flink, Airflow, …) có thể truy cập MinIO thông qua địa chỉ sau:

```text
http://minio.minio.svc.cluster.local:9000