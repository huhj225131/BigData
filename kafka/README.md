# Kafka (Kubernetes)

Mục tiêu:

- Deploy Kafka lên Minikube
- Chạy producer/consumer (dev mode)
- Consumer ghi dữ liệu Bronze lên MinIO (bucket `house-lake`, prefix `bronze/`)

Ghi chú: nếu `kubectl` chưa trỏ đúng context Minikube, bạn có thể thay `kubectl` bằng `minikube kubectl --`.

## 1) Deploy Kafka broker

Chạy từ thư mục root `BigData`:

```powershell
kubectl create namespace kafka 2>$null
kubectl apply -n kafka -f kafka/kafka.yaml
kubectl -n kafka get pods -w
```

## 2) Deploy producer + consumer (dev mode)

```powershell
kubectl apply -n kafka -f kafka/flow.yaml
kubectl -n kafka get pods -w
```

## 3) Copy code vào pods

Lấy pod names:

```powershell
$POD_CON = (kubectl get pods -n kafka -l app=consumer -o jsonpath='{.items[0].metadata.name}')
$POD_PRO = (kubectl get pods -n kafka -l app=producer -o jsonpath='{.items[0].metadata.name}')
```

Nếu bạn đang đứng ở thư mục root `BigData`:

```powershell
kubectl cp .\kafka\consumer.py -n kafka "$POD_CON`:/app/consumer.py"
kubectl cp .\kafka\upload_to_storage.py -n kafka "$POD_CON`:/app/upload_to_storage.py"

kubectl cp .\kafka\producer.py -n kafka "$POD_PRO`:/app/producer.py"
kubectl cp .\kafka\house_data.json -n kafka "$POD_PRO`:/app/house_data.json"
```

Nếu bạn đang đứng sẵn trong `BigData\kafka` thì copy như sau (không có prefix `kafka\`):

```powershell
kubectl cp .\consumer.py -n kafka "$POD_CON`:/app/consumer.py"
kubectl cp .\upload_to_storage.py -n kafka "$POD_CON`:/app/upload_to_storage.py"

kubectl cp .\producer.py -n kafka "$POD_PRO`:/app/producer.py"
kubectl cp .\house_data.json -n kafka "$POD_PRO`:/app/house_data.json"
```

## 4) Run consumer + producer

Consumer (Terminal 1):

```powershell
kubectl -n kafka exec -it deploy/consumer-logger -- python /app/consumer.py
```

Producer (Terminal 2):

```powershell
kubectl -n kafka exec -it deploy/producer-data -- python /app/producer.py
```

Dừng bằng `Ctrl + C`.

## 5) Verify Bronze on MinIO

Mở MinIO Console → bucket `house-lake` → prefix `bronze/`.

Nếu không thấy file mới:

- Check log consumer: `kubectl -n kafka logs deploy/consumer-logger --tail=200`
- Check connectivity: `kubectl -n kafka exec -it deploy/consumer-logger -- sh`
