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

# STEP 2: Schedule Batch Layer 

$batchInterval = 60  # Run batch every 60 seconds for demo
$elapsed = 0
$lastBatch = 0

try {
    while ($elapsed -lt $Duration) {
        Start-Sleep -Seconds 5
        $elapsed += 5
        
        $progress = [int](($elapsed / $Duration) * 100)
        Write-Host "Progress: $elapsed/$Duration sec ($progress%)" -ForegroundColor Cyan
        
        # Check if it's time to run batch
        if (($elapsed - $lastBatch) -ge $batchInterval) {
            Write-Host "`n[BATCH LAYER] Running batch job at $elapsed sec..." -ForegroundColor Yellow
            
            # Delete old init job if exists
            kubectl delete job batch-pipeline-init -n spark 2>&1 | Out-Null
            Start-Sleep -Seconds 2
            
            # Trigger batch pipeline
            kubectl apply -f spark/batch-pipeline-cronjob.yaml 2>&1 | Out-Null
            
            Write-Host "  Batch job triggered! Processing Bronze -> Silver -> fact_house" -ForegroundColor Green
            $lastBatch = $elapsed
        }
        
        # Check job states
        $prodState = (Get-Job -Name "producer" -ErrorAction SilentlyContinue).State
        $consState = (Get-Job -Name "consumer" -ErrorAction SilentlyContinue).State
        $streamState = (Get-Job -Name "streaming" -ErrorAction SilentlyContinue).State
        
        if ($prodState -eq "Failed" -or $consState -eq "Failed" -or $streamState -eq "Failed") {
            Write-Host "WARNING: Speed Layer job failed!" -ForegroundColor Red
        }
    }
}
catch {
    Write-Host "`nStopped by user" -ForegroundColor Yellow
}

# Stop all jobs
Get-Job | Stop-Job
Get-Job | Remove-Job -Force

Write-Host "`nFull Pipeline Completed!" -ForegroundColor Green
Write-Host "`nResults:" -ForegroundColor Cyan