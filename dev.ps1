# Tauro Development Quick Start
# Inicia backend y frontend simultáneamente

Write-Host "🚀 Starting Tauro Development Environment..." -ForegroundColor Cyan
Write-Host ""

$scriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path

# Función para matar procesos al salir
function Stop-TauroProcesses {
    Write-Host ""
    Write-Host "🛑 Stopping Tauro services..." -ForegroundColor Yellow
    Get-Job | Stop-Job
    Get-Job | Remove-Job
}

# Register cleanup
Register-EngineEvent -SourceIdentifier PowerShell.Exiting -Action { Stop-TauroProcesses }

try {
    # Start Backend
    Write-Host "▶️  Starting Backend (port 8000)..." -ForegroundColor Green
    Start-Job -Name "TauroBackend" -ScriptBlock {
        Set-Location $using:scriptDir
        python -m tauro.api.main
    } | Out-Null

    # Wait a bit for backend to start
    Start-Sleep -Seconds 3

    # Check if UI directory exists
    $uiPath = Join-Path $scriptDir "tauro\ui"
    if (-not (Test-Path $uiPath)) {
        Write-Host "❌ UI directory not found at: $uiPath" -ForegroundColor Red
        exit 1
    }

    # Install UI dependencies if needed
    $nodeModulesPath = Join-Path $uiPath "node_modules"
    if (-not (Test-Path $nodeModulesPath)) {
        Write-Host "📦 Installing UI dependencies (first time only)..." -ForegroundColor Yellow
        Set-Location $uiPath
        npm install
    }

    # Start Frontend
    Write-Host "▶️  Starting Frontend (port 3000)..." -ForegroundColor Green
    Start-Job -Name "TauroFrontend" -ScriptBlock {
        Set-Location $using:uiPath
        npm run dev
    } | Out-Null

    Write-Host ""
    Write-Host "✅ Tauro is starting!" -ForegroundColor Green
    Write-Host ""
    Write-Host "📍 Access points:" -ForegroundColor Cyan
    Write-Host "   UI:  http://localhost:3000" -ForegroundColor White
    Write-Host "   API: http://localhost:8000" -ForegroundColor White
    Write-Host "   Docs: http://localhost:8000/docs" -ForegroundColor White
    Write-Host ""
    Write-Host "⌨️  Press Ctrl+C to stop all services" -ForegroundColor Yellow
    Write-Host ""

    # Show job output in real-time
    while ($true) {
        Get-Job | Receive-Job
        Start-Sleep -Seconds 1
    }
}
catch {
    Write-Host "❌ Error: $_" -ForegroundColor Red
}
finally {
    Stop-TauroProcesses
}
