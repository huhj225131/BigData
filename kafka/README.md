# Triển khai Kafka trên Minikube, tạo luồng dữ liệu streaming
Hiện tại đang cấu hình để Producer sẽ gửi 1 dòng dữ liệu trong 3 giây, Consumer sẽ đóng gói batch 10 tin nhắn 1 để gửi cho MinIO
## 1. Triển khai Kafka Broker
### Tạo namespace
```powershell
# Tạo namespace chuyên biệt cho hệ thống lưu trữ
minikube kubectl -- create namespace kafka
```
### Triển khai broker
```powershell
# Triển khai
minikube kubectl -- apply -f kafka.yaml -n kafka
```
Sau khi triển khai, địa chỉ kết nối trong cluster kubernets sẽ là 
```text
kafka-0.kafka-service.kafka.svc.cluster.local:9092
```
## 2. Tạo producer và consumer
### Triển khai deployment producer-data và consumer-logger
```powershell
# Triển khai
minikube kubectl -- apply -f flow.yaml -n kafka
```

```powershell
# Kiểm tra trạng thái
kubectl get pods -n kafka
```
### Ghi các file code và dữ liệu vào Pod 

Vì chúng ta đang sử dụng image Python mặc định (trống), cần phải đẩy file thực thi và dữ liệu đầu vào từ máy local vào bên trong Pod để có thể chạy được luồng streaming.

#### Đẩy code cho Consumer 


```powershell
# Lấy tên Pod Consumer
$POD = (minikube kubectl -- get pods -n kafka -l app=consumer -o jsonpath='{.items[0].metadata.name}')
# Copy file vào Pod (Ghi đè nếu đã tồn tại)
minikube kubectl -- cp consumer.py kafka/${POD}:/app/consumer.py
```
#### Đẩy code và dữ liệu vào Pod Producer




```powershell
# Truy vấn và lưu tên Pod vào biến $POD_PRO
$POD= (minikube kubectl -- get pods -n kafka -l app=producer -o jsonpath='{.items[0].metadata.name}')
# Đẩy file logic xử lý gửi tin
minikube kubectl -- cp producer.py kafka/${POD}:/app/producer.py
# Đẩy file dữ liệu thô (house_data.json) dùng để mô phỏng streaming
minikube kubectl -- cp house_data.json kafka/${POD}:/app/house_data.json
```
### Chạy producer và consumer
Mở cửa sổ thứ nhất và chạy lệnh sau. Consumer sẽ bắt đầu kết nối với Kafka và sẵn sàng ghi file lên MinIO khi đủ 10 tin nhắn.
```powershell
# Chạy Consumer trong Pod
minikube kubectl -- exec -it deployment/consumer-logger -n kafka -- python /app/consumer.py
```
Mở cửa sổ thứ hai và chạy lệnh sau. Producer sẽ đọc file house_data.json và gửi dữ liệu vào Kafka mỗi 3 giây.
```powershell
# Chạy Producer trong Pod
minikube kubectl -- exec -it deployment/producer-data -n kafka -- python /app/producer.py
```