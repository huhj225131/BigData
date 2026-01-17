# 🚀 BigData Apps Launcher - Dashboard + AI Predictor

Write-Host ""
Write-Host "================================" -ForegroundColor Cyan
Write-Host "  🏠 BigData Apps Launcher" -ForegroundColor Green
Write-Host "================================" -ForegroundColor Cyan
Write-Host ""

# Start Dashboard
Write-Host "🚀 Starting Dashboard (Port 8501)..." -ForegroundColor Yellow
$dashboard = Start-Job -ScriptBlock {
    Set-Location $using:PWD
    streamlit run dashboard.py --server.port 8501 --server.headless true
}

Start-Sleep -Seconds 2

# Start AI Predictor
Write-Host "🤖 Starting AI Predictor (Port 8502)..." -ForegroundColor Yellow
$predictor = Start-Job -ScriptBlock {
    Set-Location $using:PWD
    streamlit run predict_app.py --server.port 8502 --server.headless true
}

Write-Host ""
Write-Host "✅ Apps Started!" -ForegroundColor Green
Write-Host ""
Write-Host "📊 Dashboard:    http://localhost:8501" -ForegroundColor Cyan
Write-Host "🤖 AI Predictor: http://localhost:8502" -ForegroundColor Cyan
Write-Host ""
Write-Host "Press Ctrl+C to stop" -ForegroundColor Yellow
Write-Host ""

# Wait for user to stop
try {
    while ($true) { Start-Sleep -Seconds 10 }
} finally {
    Write-Host ""
    Write-Host "🛑 Stopping apps..." -ForegroundColor Yellow
    Stop-Job -Id $dashboard.Id -ErrorAction SilentlyContinue
    Stop-Job -Id $predictor.Id -ErrorAction SilentlyContinue
    Remove-Job -Id $dashboard.Id -ErrorAction SilentlyContinue
    Remove-Job -Id $predictor.Id -ErrorAction SilentlyContinue
    Write-Host "✅ Stopped" -ForegroundColor Green
}
