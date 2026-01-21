# Postgres seeded image (house_warehouse)

Mục tiêu: đóng gói data hiện có trong Postgres (đang chạy trên Minikube) thành 1 Docker image để mọi người `docker pull` về chạy là có sẵn dữ liệu.

## Ý quan trọng

- Data bạn đang thấy trong Minikube **không nằm trong Docker image** của pod; nó nằm trong PVC mount vào `/var/lib/postgresql/data`.
- Vì vậy muốn “push docker có sẵn data” thì cách chuẩn là: **export `pg_dump` → build image từ `postgres:16` + restore trong `docker-entrypoint-initdb.d/` → push lên registry**.
- Script restore chỉ chạy khi container start lần đầu với **PGDATA trống**. Nếu bạn mount volume cũ đã có data, nó sẽ không restore lại.

## 1) Export dump từ Minikube (pg_dump)

Từ thư mục root `BigData`:

```powershell
# đảm bảo cluster đang chạy
minikube start --driver=docker
kubectl apply -f .\postgres\postgres.yaml
kubectl -n postgres rollout status deploy/postgres --timeout=180s

# dump database (custom format) vào /tmp trong pod
kubectl -n postgres exec deploy/postgres -- sh -lc "PGPASSWORD=postgres pg_dump -h localhost -U postgres -d house_warehouse -Fc -f /tmp/house_warehouse.dump"

# copy dump về local
$POD=(kubectl -n postgres get pods -l app=postgres -o jsonpath='{.items[0].metadata.name}')
kubectl -n postgres cp "$POD`:/tmp/house_warehouse.dump" .\docker\postgres-seeded\seed\house_warehouse.dump
```

## 2) Build image

```powershell
docker build -t house-warehouse-seeded:2026-01-16 .\docker\postgres-seeded
```

## 3) Test chạy local

```powershell
# chạy fresh (không dùng volume cũ)
docker rm -f house-warehouse 2>$null

docker run --name house-warehouse \
  -e POSTGRES_USER=postgres \
  -e POSTGRES_PASSWORD=postgres \
  -e POSTGRES_DB=house_warehouse \
  -p 5432:5432 \
  house-warehouse-seeded:2026-01-16
```

## 4) Tag + push lên Docker Hub (hoặc registry khác)

```powershell
# Docker Hub: huytq2809/house-warehouse-seeded
docker tag house-warehouse-seeded:2026-01-16 huytq2809/house-warehouse-seeded:2026-01-16
docker tag house-warehouse-seeded:2026-01-16 huytq2809/house-warehouse-seeded:latest

docker login

docker push huytq2809/house-warehouse-seeded:2026-01-16
docker push huytq2809/house-warehouse-seeded:latest
```

Người khác dùng:

```powershell
docker pull huytq2809/house-warehouse-seeded:2026-01-16

docker run --name house-warehouse \
  -e POSTGRES_USER=postgres \
  -e POSTGRES_PASSWORD=postgres \
  -e POSTGRES_DB=house_warehouse \
  -p 5432:5432 \
  huytq2809/house-warehouse-seeded:2026-01-16
```
