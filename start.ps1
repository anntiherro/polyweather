$ROOT = Split-Path -Parent $MyInvocation.MyCommand.Path
$MONITOR = "$ROOT\prediction-market-monitor"
$PYTHON = "$ROOT\.venv\Scripts\python.exe"
$JAR = "$MONITOR\flink\jobs\target\correlation-job-1.0-shaded.jar"

Write-Host "=== 1. Docker Compose ===" -ForegroundColor Cyan
Set-Location $MONITOR
docker compose up -d
if ($LASTEXITCODE -ne 0) { Write-Host "Docker failed" -ForegroundColor Red; exit 1 }

Write-Host "`n=== 2. Waiting for Kafka and Flink ===" -ForegroundColor Cyan
$ready = $false
for ($i = 0; $i -lt 20; $i++) {
    Start-Sleep 3
    try {
        $overview = Invoke-RestMethod "http://localhost:8081/overview" -ErrorAction Stop
        if ($overview.'slots-total' -gt 0) { $ready = $true; break }
    } catch {}
    Write-Host "  waiting... ($($i*3)s)"
}
if (-not $ready) { Write-Host "Flink not ready after 60s" -ForegroundColor Red; exit 1 }
Write-Host "  Flink ready: $($overview.'slots-total') slots" -ForegroundColor Green

Write-Host "`n=== 3. Build JAR (if sources changed) ===" -ForegroundColor Cyan
Set-Location "$MONITOR\flink\jobs"
mvn package -q
if ($LASTEXITCODE -ne 0) { Write-Host "Maven build failed" -ForegroundColor Red; exit 1 }
Write-Host "  JAR built OK" -ForegroundColor Green

Write-Host "`n=== 4. Upload JAR to Flink ===" -ForegroundColor Cyan
$upload = Invoke-RestMethod -Uri "http://localhost:8081/jars/upload" `
    -Method Post `
    -Form @{ jarfile = Get-Item $JAR }
$jarId = $upload.filename.Split("/")[-1]
Write-Host "  Uploaded: $jarId" -ForegroundColor Green

Write-Host "`n=== 5. Submit Flink jobs ===" -ForegroundColor Cyan
$jobs = @("com.polyweather.CorrelationJob", "com.polyweather.AccuracyJob", "com.polyweather.AnomalyJob")
foreach ($cls in $jobs) {
    $resp = Invoke-RestMethod -Uri "http://localhost:8081/jars/$jarId/run" `
        -Method Post -ContentType "application/json" `
        -Body "{`"entryClass`":`"$cls`"}"
    Write-Host "  $cls -> jobid=$($resp.jobid)" -ForegroundColor Green
}

Write-Host "`n=== 6. Start producers and pg_consumer ===" -ForegroundColor Cyan
Start-Process -FilePath $PYTHON -ArgumentList "$MONITOR\producers\polymarket_producer.py" -WindowStyle Normal
Write-Host "  polymarket_producer started" -ForegroundColor Green
Start-Process -FilePath $PYTHON -ArgumentList "$MONITOR\producers\weather_producer.py" -WindowStyle Normal
Write-Host "  weather_producer started" -ForegroundColor Green
Start-Process -FilePath $PYTHON -ArgumentList "$MONITOR\producers\pg_consumer.py" -WindowStyle Normal
Write-Host "  pg_consumer started" -ForegroundColor Green

Write-Host "`n=== All done ===" -ForegroundColor Green
Write-Host "  Kafka UI  : http://localhost:8080"
Write-Host "  Flink UI  : http://localhost:8081"
Write-Host "  PostgreSQL: localhost:5432 / db=polyweather user=polyweather"
