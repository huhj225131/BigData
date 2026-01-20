# Hệ Thống Phân Tích Bất Động Sản BigData - Hướng Dẫn Nhanh

## Yêu Cầu
- Docker Desktop + Minikube + kubectl
---

## Cài Đặt

```bash
# 1. Clone repo
git clone <your-repo-url>
cd BigData

# 2. Start Minikube
minikube start --memory=8192 --cpus=4

# 3. Deploy hạ tầng
kubectl apply -f postgres/postgres.yaml
kubectl apply -f minio/config_minio.yaml
kubectl apply -f kafka/kafka.yaml
kubectl apply -f spark/spark-runner.k8s.yaml

# 4. Setup Kafka pipeline
.\setup_pipeline.ps1

# 5. Chạy Batch Processing (tạo Gold tables + ML model)
kubectl apply -f spark/house-price-train-job.yaml

# 6. Deploy Dashboard & Predictor
kubectl apply -f dashboard-deployment.yaml
kubectl apply -f predict-deployment.yaml

# 7. Port forward
.\port_forward_all.ps1
```

---

## Truy Cập

- **Dashboard**: http://localhost:8501
- **Predictor**: http://localhost:8502
- **MinIO**: http://localhost:9001 (minioadmin/minioadmin)
- **PostgreSQL**: localhost:5433 (postgres/postgres)

---

## Dọn Dẹp

```powershell
# Dừng port forwards
.\cleanup.ps1

# Xóa deployments
kubectl delete -f dashboard-deployment.yaml
kubectl delete -f predict-deployment.yaml
kubectl delete -f spark/spark-runner.k8s.yaml
kubectl delete -f kafka/kafka.yaml
kubectl delete -f minio/config_minio.yaml
kubectl delete -f postgres/postgres.yaml

# Xóa Minikube
minikube delete
```