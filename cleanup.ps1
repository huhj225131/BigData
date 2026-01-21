
Write-Host ""
Write-Host "=" * 60 -ForegroundColor Red
Write-Host "  ⚠️  CLEANUP SCRIPT - XÓA TẤT CẢ DỮ LIỆU" -ForegroundColor Red
Write-Host "=" * 60 -ForegroundColor Red
Write-Host ""

Write-Host "Stopping port-forward jobs..." -ForegroundColor Yellow
$jobs = Get-Job -ErrorAction SilentlyContinue
if ($jobs) {
    $jobs | Stop-Job -ErrorAction SilentlyContinue
    $jobs | Remove-Job -ErrorAction SilentlyContinue
    Write-Host "Stopped $($jobs.Count) port-forward jobs" -ForegroundColor Green
}

Write-Host ""
Write-Host "Script này sẽ xóa:" -ForegroundColor Yellow
Write-Host "  - Tất cả pods trong kafka, minio, postgres, spark namespaces" -ForegroundColor Yellow
Write-Host "  - Tất cả deployments, services, statefulsets" -ForegroundColor Yellow
Write-Host "  - Tất cả PersistentVolumeClaims (DỮ LIỆU LƯU TRỮ)" -ForegroundColor Yellow
Write-Host "  - Tất cả ConfigMaps" -ForegroundColor Yellow
Write-Host "  - Tất cả Jobs và CronJobs" -ForegroundColor Yellow
Write-Host ""

$confirm = Read-Host "Bạn có chắc muốn tiếp tục? (yes/no)"

if ($confirm -ne "yes") {
    Write-Host ""
    Write-Host "❌ Đã hủy cleanup" -ForegroundColor Red
    exit 0
}

Write-Host ""
Write-Host "  Bắt đầu cleanup..." -ForegroundColor Yellow
Write-Host ""

Write-Host "[1/5] Xóa resources trong kafka namespace..." -ForegroundColor Cyan
kubectl delete all --all -n kafka 2>$null
kubectl delete configmap --all -n kafka 2>$null
kubectl delete pvc --all -n kafka 2>$null

Write-Host "[2/5] Xóa resources trong spark namespace..." -ForegroundColor Cyan
kubectl delete all --all -n spark 2>$null
kubectl delete configmap --all -n spark 2>$null
kubectl delete cronjob --all -n spark 2>$null
kubectl delete job --all -n spark 2>$null
kubectl delete pvc --all -n spark 2>$null

Write-Host "[3/5] Xóa resources trong minio namespace..." -ForegroundColor Cyan
kubectl delete all --all -n minio 2>$null
kubectl delete configmap --all -n minio 2>$null
kubectl delete pvc --all -n minio 2>$null

Write-Host "[4/5] Xóa resources trong postgres namespace..." -ForegroundColor Cyan
kubectl delete all --all -n postgres 2>$null
kubectl delete configmap --all -n postgres 2>$null
kubectl delete pvc --all -n postgres 2>$null

Write-Host "[5/5] Xóa PVCs cũ trong default namespace..." -ForegroundColor Cyan
kubectl get pvc -n default -o name 2>$null | ForEach-Object {
    $pvcName = $_ -replace "persistentvolumeclaim/", ""
    if ($pvcName -like "*minio*" -or $pvcName -like "*kafka*" -or $pvcName -like "*postgres*" -or $pvcName -like "*spark*") {
        kubectl delete pvc $pvcName -n default --ignore-not-found=true 2>$null
    }
}

Write-Host ""
Write-Host " Đợi resources cleanup..." -ForegroundColor Yellow
Start-Sleep -Seconds 5

# Verify cleanup
Write-Host ""
Write-Host "=" * 60 -ForegroundColor Green
Write-Host "   CLEANUP HOÀN TẤT" -ForegroundColor Green
Write-Host "=" * 60 -ForegroundColor Green
Write-Host ""

Write-Host " Trạng thái hiện tại:" -ForegroundColor Cyan
Write-Host ""

$namespaces = @("kafka", "minio", "postgres", "spark")
foreach ($ns in $namespaces) {
    $podCount = (kubectl get pods -n $ns 2>$null | Measure-Object).Count - 1
    if ($podCount -lt 0) { $podCount = 0 }
    $status = if ($podCount -eq 0) { "✅" } else { "⚠️ " }
    Write-Host "  $status $ns`: $podCount pods" -ForegroundColor $(if ($podCount -eq 0) { "Green" } else { "Yellow" })
}

Write-Host ""

$totalPvcs = (kubectl get pvc -A 2>$null | Select-String -Pattern "(kafka|minio|postgres|spark)" | Measure-Object).Count
Write-Host "  $(if ($totalPvcs -eq 0) { '✅' } else { '⚠️ ' }) Total PVCs: $totalPvcs" -ForegroundColor $(if ($totalPvcs -eq 0) { "Green" } else { "Yellow" })

Write-Host ""
Write-Host " Cluster đã sạch sẽ! Sẵn sàng deploy lại." -ForegroundColor Green
Write-Host ""
Write-Host " Để deploy lại, xem: QUICK_START.md" -ForegroundColor Cyan
Write-Host ""
