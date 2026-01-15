# PostgreSQL trên Kubernetes

## 1) Deploy

Ghi chú: nếu `kubectl` chưa trỏ đúng context Minikube, bạn có thể thay `kubectl` bằng `minikube kubectl --`.

```powershell
# Apply toàn bộ manifest (Namespace + Secret + PVC + Deployment + Service)
kubectl apply -f postgres/postgres.yaml

# Chờ Pod Running
kubectl -n postgres get pods
```

## 2) Thông tin kết nối nội bộ (trong cluster)

- Host: `postgres.postgres.svc.cluster.local`
- Port: `5432`
- Database: `house_warehouse`
- User/Pass: `postgres` / `postgres`

JDBC URL (Spark):

```text
jdbc:postgresql://postgres.postgres.svc.cluster.local:5432/house_warehouse
```

## 3) Mở cổng ra ngoài (optional)

```powershell
kubectl -n postgres port-forward svc/postgres 5432:5432
```

Sau đó từ máy local có thể dùng:

```text
jdbc:postgresql://localhost:5432/house_warehouse
```
