# Inicia Kafka, topics, consumer, 3 producers, Streamlit y el monitor de datos.
param([switch]$SinNavegador)

$ErrorActionPreference = "Stop"
$Root = Split-Path -Parent $PSScriptRoot
Set-Location $Root

$Python = Join-Path $Root ".venv\Scripts\python.exe"
if (-not (Test-Path $Python)) {
    Write-Host "No existe .venv. Creando el entorno virtual..."
    if (-not (Get-Command py -ErrorAction SilentlyContinue)) {
        throw "No se encontro Python. Instala Python 3.11 o superior y vuelve a ejecutar INICIAR.bat."
    }
    py -3 -m venv .venv
    if ($LASTEXITCODE -ne 0) { throw "No se pudo crear el entorno virtual." }
}

# Un entorno puede existir incompleto o tener kafka-python instalado junto con
# kafka-python-ng. Ambos usan el paquete "kafka" y no pueden coexistir.
$DependencyProbe = "import kafka, pandas, streamlit, plotly, supabase, dotenv; from kafka.errors import NoBrokersAvailable; assert kafka.__version__ == '2.2.3'"
$PreviousErrorPreference = $ErrorActionPreference
$ErrorActionPreference = "Continue"
& $Python -c $DependencyProbe 2>$null
$DependencyProbeExitCode = $LASTEXITCODE
$ErrorActionPreference = $PreviousErrorPreference
if ($DependencyProbeExitCode -ne 0) {
    Write-Host "Instalando o reparando dependencias de Python..."
    & $Python -m pip install --upgrade pip
    & $Python -c "from importlib.metadata import distributions; import sys; sys.exit(0 if any(d.metadata['Name'] == 'kafka-python' for d in distributions()) else 1)"
    if ($LASTEXITCODE -eq 0) {
        Write-Host "Quitando kafka-python porque entra en conflicto con kafka-python-ng..."
        & $Python -m pip uninstall -y kafka-python
    }
    & $Python -m pip install --force-reinstall --no-deps kafka-python-ng==2.2.3
    & $Python -m pip install -r (Join-Path $Root "requirements.txt")
    if ($LASTEXITCODE -ne 0) {
        throw "No se pudieron instalar las dependencias de requirements.txt."
    }
    & $Python -c $DependencyProbe
    if ($LASTEXITCODE -ne 0) {
        throw "No se pudo reparar el cliente de Kafka para Python."
    }
}
& $Python -m pip check
if ($LASTEXITCODE -ne 0) { throw "El entorno Python tiene dependencias incompatibles." }

New-Item -ItemType Directory -Force -Path (Join-Path $Root "logs") | Out-Null
New-Item -ItemType Directory -Force -Path (Join-Path $Root "datos") | Out-Null

Write-Host "=== 1. Levantando Kafka (localhost:9092) ==="
$KafkaConDocker = $false
$Docker = Get-Command docker -ErrorAction SilentlyContinue
if ($Docker) {
    # Windows PowerShell convierte stderr nativo en errores; evaluar el codigo.
    $ErrorActionPreference = "Continue"
    docker info *> $null
    if ($LASTEXITCODE -ne 0) {
        $DockerDesktop = "C:\Program Files\Docker\Docker\Docker Desktop.exe"
        if (Test-Path $DockerDesktop) {
            Write-Host "Iniciando Docker Desktop..."
            Start-Process -FilePath $DockerDesktop -WindowStyle Hidden | Out-Null
            foreach ($intento in 1..30) {
                Start-Sleep -Seconds 2
                docker info *> $null
                if ($LASTEXITCODE -eq 0) { break }
            }
        }
    }
    docker info *> $null
    if ($LASTEXITCODE -eq 0) {
        docker compose up -d
        $KafkaConDocker = ($LASTEXITCODE -eq 0)
    }
    $ErrorActionPreference = "Stop"
}

if (-not $KafkaConDocker) {
    & powershell -NoProfile -ExecutionPolicy Bypass -File (Join-Path $Root "scripts\iniciar_kafka_local.ps1")
    if ($LASTEXITCODE -ne 0) { throw "No se pudo iniciar Kafka." }
}

Write-Host "=== 2. Creando topics (ventas, clientes, pagos, inventario) ==="
& $Python (Join-Path $Root "scripts\crear_topics.py")
if ($LASTEXITCODE -ne 0) {
    throw "No se pudieron crear los topics."
}

function Start-ObelieProcess {
    param(
        [string]$Name,
        [string[]]$ArgumentList
    )
    $out = Join-Path $Root "logs\$Name.out.log"
    $err = Join-Path $Root "logs\$Name.err.log"
    $pidFile = Join-Path $Root "logs\$Name.pid"
    $marker = if ($Name -eq "dashboard") { "dashboard.py" } else { "$Name.py" }
    if (Test-Path $pidFile) {
        $existingPid = 0
        if ([int]::TryParse((Get-Content $pidFile -Raw).Trim(), [ref]$existingPid)) {
            $known = Get-CimInstance Win32_Process -Filter "ProcessId=$existingPid" -ErrorAction SilentlyContinue
            if ($known -and $known.ExecutablePath -eq $Python -and $known.CommandLine -and $known.CommandLine.Contains($marker)) {
                Write-Host ("{0} ya esta activo. PID={1}" -f $Name, $existingPid)
                return
            }
        }
    }

    # Adopta una instancia iniciada manualmente o por una ejecucion anterior.
    $running = Get-CimInstance Win32_Process | Where-Object {
        $_.ExecutablePath -and $_.ExecutablePath.Equals($Python, [System.StringComparison]::OrdinalIgnoreCase) -and
        $_.CommandLine -and $_.CommandLine.Contains($marker)
    } | Sort-Object CreationDate | Select-Object -First 1
    if ($running) {
        Set-Content -Path $pidFile -Value $running.ProcessId -Encoding ASCII
        Write-Host ("{0} ya estaba activo. PID={1}" -f $Name, $running.ProcessId)
        return
    }

    $proc = Start-Process -FilePath $Python -ArgumentList $ArgumentList -WorkingDirectory $Root `
        -RedirectStandardOutput $out -RedirectStandardError $err -WindowStyle Hidden -PassThru
    Set-Content -Path $pidFile -Value $proc.Id -Encoding ASCII
    Write-Host ("Iniciado {0} PID={1}" -f $Name, $proc.Id)
}

Write-Host "=== 3. Iniciando consumer unico ==="
Start-ObelieProcess -Name "consumer" -ArgumentList @("consumer\consumer.py")
Start-Sleep -Seconds 2

Write-Host "=== 4. Iniciando 3 producers ==="
Start-ObelieProcess -Name "producer_1" -ArgumentList @("producers\producer_1.py")
Start-ObelieProcess -Name "producer_2" -ArgumentList @("producers\producer_2.py")
Start-ObelieProcess -Name "producer_3" -ArgumentList @("producers\producer_3.py")

Write-Host "=== 5. Iniciando dashboard Streamlit ==="
Start-ObelieProcess -Name "dashboard" -ArgumentList @("-m", "streamlit", "run", "dashboard\dashboard.py", "--server.headless", "true", "--server.port", "8501")

Write-Host "=== 6. Comprobando dashboard y recepcion de datos nuevos ==="
& $Python (Join-Path $Root "scripts\esperar_inicio.py")
if ($LASTEXITCODE -ne 0) {
    throw "El inicio no se completo. Revisa el error anterior y los archivos de logs."
}
foreach ($Name in @("consumer", "producer_1", "producer_2", "producer_3", "dashboard")) {
    $processId = [int](Get-Content (Join-Path $Root "logs\$Name.pid") -Raw).Trim()
    $running = Get-CimInstance Win32_Process -Filter "ProcessId=$processId" -ErrorAction SilentlyContinue
    $marker = "$Name.py"
    if (-not $running -or $running.ExecutablePath -ne $Python -or -not $running.CommandLine.Contains($marker)) {
        throw "$Name no sigue activo. Revisa logs\$Name.err.log."
    }
}

Write-Host ""
Write-Host "Sistema iniciado. Kafka + Supabase."
Write-Host "Dashboard: http://localhost:8501"
if (-not $SinNavegador) {
    Start-Process "http://localhost:8501"
}
Write-Host ""
& $Python (Join-Path $Root "scripts\mostrar_datos.py")
