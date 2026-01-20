# Port Forward All Services
# Chay script nay de truy cap PostgreSQL, MinIO, Dashboard, Predictor

Write-Host "============================================" -ForegroundColor Cyan
Write-Host "Starting Port Forwards for All Services" -ForegroundColor Cyan  
Write-Host "============================================" -ForegroundColor Cyan

# Clean up old jobs first
Write-Host "`nCleaning up old port-forward jobs..." -ForegroundColor Yellow
Get-Job | Stop-Job -ErrorAction SilentlyContinue
Get-Job | Remove-Job -Force -ErrorAction SilentlyContinue

# Start port forwards in background jobs
Write-Host "`nStarting services..." -ForegroundColor Yellow

Write-Host "PostgreSQL (port 5433)..." -ForegroundColor White
Start-Job -Name "postgres" -ScriptBlock { kubectl -n postgres port-forward svc/postgres 5433:5432 } | Out-Null

Write-Host "MinIO API (port 9000)..." -ForegroundColor White
Start-Job -Name "minio-api" -ScriptBlock { kubectl -n minio port-forward svc/minio-public 9000:9000 } | Out-Null

Write-Host "MinIO Console (port 9001)..." -ForegroundColor White
Start-Job -Name "minio-console" -ScriptBlock { kubectl -n minio port-forward svc/minio-public 9001:9001 } | Out-Null

# Write-Host "Dashboard (port 8501)..." -ForegroundColor White
# Start-Job -Name "dashboard" -ScriptBlock { kubectl -n default port-forward svc/house-dashboard 8501:8501 } | Out-Null

Write-Host "Predictor (port 8502)..." -ForegroundColor White
Start-Job -Name "predictor" -ScriptBlock { kubectl -n default port-forward svc/house-predictor 8502:8502 } | Out-Null

Start-Sleep -Seconds 2

Write-Host "`nAll services started!" -ForegroundColor Green
Write-Host "`n============================================" -ForegroundColor Cyan
Write-Host "Access URLs:" -ForegroundColor Cyan
Write-Host "============================================" -ForegroundColor Cyan
Write-Host "PostgreSQL:  localhost:5433 (DBeaver)" -ForegroundColor White
Write-Host "             User: postgres, Pass: postgres" -ForegroundColor Gray
Write-Host "             Database: house_warehouse" -ForegroundColor Gray
Write-Host ""
Write-Host "MinIO API:   localhost:9000" -ForegroundColor White
Write-Host "MinIO UI:    http://localhost:9001" -ForegroundColor White
Write-Host "             User: minioadmin, Pass: minioadmin" -ForegroundColor Gray
Write-Host ""
Write-Host "Dashboard:   http://localhost:8501" -ForegroundColor White
Write-Host "Predictor:   http://localhost:8502" -ForegroundColor White
Write-Host "============================================" -ForegroundColor Cyan

Write-Host "`nRunning jobs:" -ForegroundColor Yellow
Get-Job | Format-Table -Property Id,Name,State

Write-Host "`nPress Ctrl+C to stop all port forwards" -ForegroundColor Yellow
Write-Host "Or run: .\cleanup.ps1" -ForegroundColor Gray

# Keep running until user stops
try {
    while ($true) { 
        Start-Sleep -Seconds 10 
        # Check if any job failed
        $failed = Get-Job | Where-Object { $_.State -eq "Failed" }
        if ($failed) {
            Write-Host "`nWarning: Some jobs failed. Restarting..." -ForegroundColor Red
            # Could implement restart logic here
        }
    }
} finally {
    Write-Host "`nStopping all services..." -ForegroundColor Yellow
    Get-Job | Stop-Job -ErrorAction SilentlyContinue
    Get-Job | Remove-Job -ErrorAction SilentlyContinue
    Write-Host "Stopped" -ForegroundColor Green
}
