# Detiene producers, consumer, Streamlit y opcionalmente Kafka.
$Root = Split-Path -Parent $PSScriptRoot
$Python = Join-Path $Root ".venv\Scripts\python.exe"
Set-Location $Root

Write-Host "Deteniendo procesos Python del proyecto Obelie..."
function Stop-ProcessTree {
    param([int]$Id)
    $children = Get-CimInstance Win32_Process -Filter "ParentProcessId=$Id" -ErrorAction SilentlyContinue
    foreach ($child in $children) {
        Stop-ProcessTree -Id $child.ProcessId
    }
    Stop-Process -Id $Id -Force -ErrorAction SilentlyContinue
}

$nombres = @("consumer", "producer_1", "producer_2", "producer_3", "dashboard")
foreach ($nombre in $nombres) {
    $pidFile = Join-Path $Root "logs\$nombre.pid"
    if (-not (Test-Path $pidFile)) { continue }
    $processId = 0
    if ([int]::TryParse((Get-Content $pidFile -Raw).Trim(), [ref]$processId)) {
        $known = Get-CimInstance Win32_Process -Filter "ProcessId=$processId" -ErrorAction SilentlyContinue
        if ($known -and $known.ExecutablePath -eq $Python -and $known.CommandLine -and $known.CommandLine.Contains("$nombre.py")) {
            Write-Host ("Stop PID {0} :: {1}" -f $processId, $nombre)
            Stop-ProcessTree -Id $processId
        }
    }
    Remove-Item -LiteralPath $pidFile -Force -ErrorAction SilentlyContinue
}

if ($args -contains "-Kafka") {
    $docker = Get-Command docker -ErrorAction SilentlyContinue
    if ($docker) {
        docker info *> $null
        if ($LASTEXITCODE -eq 0) {
            Write-Host "Deteniendo Kafka de Docker..."
            docker compose down
        }
    }

    $kafkaPidFile = Join-Path $Root "logs\kafka-local.pid"
    if (Test-Path $kafkaPidFile) {
        $kafkaPid = 0
        if ([int]::TryParse((Get-Content $kafkaPidFile -Raw).Trim(), [ref]$kafkaPid)) {
            $known = Get-CimInstance Win32_Process -Filter "ProcessId=$kafkaPid" -ErrorAction SilentlyContinue
            if ($known -and $known.Name -eq "java.exe" -and $known.CommandLine -and $known.CommandLine.Contains("obelie-server.properties")) {
                Write-Host ("Deteniendo Kafka local PID={0}..." -f $kafkaPid)
                Stop-ProcessTree -Id $kafkaPid
            }
        }
        Remove-Item -LiteralPath $kafkaPidFile -Force -ErrorAction SilentlyContinue
    }
}

Write-Host "Listo."
