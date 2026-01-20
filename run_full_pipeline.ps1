param(
    [int]$Duration = 600  # 10 minutes default
)

# STEP 1: Start Speed Layer
$POD_PRO = kubectl get pods -n kafka -l app=producer -o jsonpath='{.items[0].metadata.name}'
$POD_CON = kubectl get pods -n kafka -l app=consumer -o jsonpath='{.items[0].metadata.name}'
$POD_STREAM = kubectl get pods -n kafka -l app=spark-streaming -o jsonpath='{.items[0].metadata.name}'

# Start Producer
$producerJob = Start-Job -Name "producer" -ScriptBlock {
    param($pod, $duration)
    kubectl exec -n kafka $pod -- sh -lc "export INTERVAL=3; export REPEAT=1; timeout $duration python /app/producer.py"
} -ArgumentList $POD_PRO, $Duration

Start-Sleep -Seconds 3

# Start Consumer
$consumerJob = Start-Job -Name "consumer" -ScriptBlock {
    param($pod, $duration)
    kubectl exec -n kafka $pod -- timeout $duration python /app/consumer.py
} -ArgumentList $POD_CON, $Duration

Start-Sleep -Seconds 3

# Start Spark Streaming
$streamJob = Start-Job -Name "streaming" -ScriptBlock {
    param($pod, $duration)
    kubectl exec -n kafka $pod -- /bin/bash -c "mkdir -p /tmp/ivy2 && timeout $duration /opt/spark/bin/spark-submit --master local[2] --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,org.postgresql:postgresql:42.7.1 --conf spark.jars.ivy=/tmp/ivy2 /app/stream.py"
} -ArgumentList $POD_STREAM, $Duration


# Stop all jobs
Get-Job | Stop-Job
Get-Job | Remove-Job -Force

Write-Host "`nFull Pipeline Completed!" -ForegroundColor Green
Write-Host "`nResults:" -ForegroundColor Cyan