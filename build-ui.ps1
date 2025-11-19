# Build script para Tauro UI (Windows)

Write-Host "🏗️  Building Tauro UI..." -ForegroundColor Cyan

# Change to UI directory
$scriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
Set-Location "$scriptDir\tauro\ui"

# Install dependencies if needed
if (-not (Test-Path "node_modules")) {
    Write-Host "📦 Installing dependencies..." -ForegroundColor Yellow
    npm install
}

# Build
Write-Host "⚙️  Running production build..." -ForegroundColor Yellow
npm run build

Write-Host ""
Write-Host "✅ Build complete! Output in: tauro/ui/dist/" -ForegroundColor Green
Write-Host ""
Write-Host "To serve from FastAPI, visit: http://localhost:8000/ui" -ForegroundColor Cyan
