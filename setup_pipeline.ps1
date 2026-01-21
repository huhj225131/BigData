
$POD_PRO = kubectl get pods -n kafka -l app=producer -o jsonpath='{.items[0].metadata.name}'
$POD_CON = kubectl get pods -n kafka -l app=consumer -o jsonpath='{.items[0].metadata.name}'
$POD_STREAM = kubectl get pods -n kafka -l app=spark-streaming -o jsonpath='{.items[0].metadata.name}'

kubectl cp kafka/producer.py -n kafka "${POD_PRO}:/app/producer.py"
kubectl cp kafka/house_data.json -n kafka "${POD_PRO}:/app/house_data.json"

kubectl cp kafka/consumer.py -n kafka "${POD_CON}:/app/consumer.py"
kubectl cp kafka/upload_to_storage.py -n kafka "${POD_CON}:/app/upload_to_storage.py"

kubectl cp spark_streaming/stream.py -n kafka "${POD_STREAM}:/app/stream.py"

Write-Host "`nCode copied successfully!" -ForegroundColor Green
