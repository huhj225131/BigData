
param(
    [int]$Duration = 120
)
Write-Host "`nGetting pod names..." -ForegroundColor Yellow
$POD_PRO = kubectl get pods -n kafka -l app=producer -o jsonpath='{.items[0].metadata.name}'
$POD_CON = kubectl get pods -n kafka -l app=consumer -o jsonpath='{.items[0].metadata.name}'
$POD_STREAM = kubectl get pods -n kafka -l app=spark-streaming -o jsonpath='{.items[0].metadata.name}'

Write-Host "`nStarting Producer..." -ForegroundColor Green
$producerJob = Start-Job -Name "producer" -ScriptBlock {
    param($pod, $duration)
    kubectl exec -n kafka $pod -- sh -lc "export INTERVAL=3; export REPEAT=1; timeout $duration python /app/producer.py"
} -ArgumentList $POD_PRO, $Duration

Start-Sleep -Seconds 3

Write-Host "Starting Consumer..." -ForegroundColor Green
$consumerJob = Start-Job -Name "consumer" -ScriptBlock {
    param($pod, $duration)
    kubectl exec -n kafka $pod -- timeout $duration python /app/consumer.py
} -ArgumentList $POD_CON, $Duration

Start-Sleep -Seconds 3

$streamJob = Start-Job -Name "streaming" -ScriptBlock {
    param($pod, $duration)
    kubectl exec -n kafka $pod -- /bin/bash -c "mkdir -p /tmp/ivy2 && timeout $duration /opt/spark/bin/spark-submit --master local[2] --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,org.postgresql:postgresql:42.7.1 --conf spark.jars.ivy=/tmp/ivy2 /app/stream.py"
} -ArgumentList $POD_STREAM, $Duration

Write-Host "`nAll services started!" -ForegroundColor Green

$elapsed = 0
$interval = 5

try {
    while ($elapsed -lt $Duration) {
        Start-Sleep -Seconds $interval
        $elapsed += $interval
        
        $progress = [int](($elapsed / $Duration) * 100)
        Write-Host "Progress: $elapsed/$Duration sec ($progress%)" -ForegroundColor Yellow
        
        $prodState = (Get-Job -Name "producer").State
        $consState = (Get-Job -Name "consumer").State
        $streamState = (Get-Job -Name "streaming").State
        
        if ($prodState -eq "Failed" -or $consState -eq "Failed" -or $streamState -eq "Failed") {
            Write-Host "WARNING: One or more jobs failed!" -ForegroundColor Red
            break
        }
    }
} catch {
    Write-Host "`nInterrupted by user" -ForegroundColor Yellow
}

Write-Host "`nStopping all services..." -ForegroundColor Yellow

Write-Host "`nProducer Output:" -ForegroundColor Cyan
Receive-Job -Name "producer" | Select-Object -Last 10

Write-Host "`nConsumer Output:" -ForegroundColor Cyan
Receive-Job -Name "consumer" | Select-Object -Last 10

Write-Host "`nStreaming Output:" -ForegroundColor Cyan
Receive-Job -Name "streaming" | Select-Object -Last 10

Stop-Job -Name "producer","consumer","streaming" -ErrorAction SilentlyContinue
Remove-Job -Name "producer","consumer","streaming" -ErrorAction SilentlyContinue

Write-Host "`nPipeline test completed!" -ForegroundColor Green