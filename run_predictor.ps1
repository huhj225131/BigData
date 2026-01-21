# 🤖 Quick Start - AI House Price Predictor

Write-Host ""
Write-Host "🤖 Starting AI House Price Predictor..." -ForegroundColor Cyan
Write-Host ""

# Check dependencies
Write-Host "Checking dependencies..." -ForegroundColor Yellow

$required = @("streamlit", "pandas", "numpy")
foreach ($pkg in $required) {
    try {
        python -c "import $pkg" 2>$null
        Write-Host "  ✅ $pkg" -ForegroundColor Green
    } catch {
        Write-Host "  ⚠️  $pkg not found, installing..." -ForegroundColor Yellow
        pip install $pkg -q
    }
}

Write-Host ""
Write-Host "=" * 60 -ForegroundColor Green
Write-Host "  🚀 Launching AI Predictor" -ForegroundColor Green
Write-Host "=" * 60 -ForegroundColor Green
Write-Host ""
Write-Host "  URL: http://localhost:8502" -ForegroundColor Cyan
Write-Host "  Press Ctrl+C to stop" -ForegroundColor Yellow
Write-Host ""

# Run
streamlit run predict_app.py --server.port 8502
