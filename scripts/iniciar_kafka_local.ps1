# Inicia Apache Kafka 3.8.1 en modo KRaft sin Docker.
# Los binarios y datos se guardan fuera del proyecto, en LOCALAPPDATA\ObelieKafka.
$ErrorActionPreference = "Stop"
$Root = Split-Path -Parent $PSScriptRoot
$Logs = Join-Path $Root "logs"
New-Item -ItemType Directory -Force -Path $Logs | Out-Null

function Test-KafkaPort {
    $client = New-Object System.Net.Sockets.TcpClient
    try {
        $task = $client.ConnectAsync("127.0.0.1", 9092)
        return $task.Wait(700) -and $client.Connected
    }
    catch {
        return $false
    }
    finally {
        $client.Dispose()
    }
}

if (-not (Get-Command java -ErrorAction SilentlyContinue)) {
    throw "Kafka local necesita Java 17 o superior y no se encontro el comando java."
}
if (-not $env:LOCALAPPDATA) {
    throw "No se encontro la variable LOCALAPPDATA."
}

$Version = "3.8.1"
$Package = "kafka_2.13-$Version"
$PreferredBase = Join-Path $env:LOCALAPPDATA "ObelieKafka"
$ExistingBase = Join-Path $env:LOCALAPPDATA "obelie-kafka"
$Base = $PreferredBase
if (Test-Path (Join-Path $ExistingBase "$Package\bin\windows\kafka-server-start.bat")) {
    $Base = $ExistingBase
}
$KafkaHome = Join-Path $Base $Package
$Archive = Join-Path $Base "$Package.tgz"
$Url = "https://archive.apache.org/dist/kafka/$Version/$Package.tgz"
$ExpectedSha512 = "B43FADA353B7DCA51C0F90ACF594EC1CE06B2344C046D4059D4DEAB0615E0E3E76E92ECCDBDFA1ADAD1FBDE76C5F25E71ACD0DB013FB4B1778827448B5285EDF"

New-Item -ItemType Directory -Force -Path $Base | Out-Null

if (Test-KafkaPort) {
    $ExistingBroker = Get-CimInstance Win32_Process | Where-Object {
        $_.Name -match "^java" -and $_.CommandLine -and $_.CommandLine.Contains("kafka.Kafka") -and
        $_.CommandLine.Contains((Join-Path $Base "obelie-server.properties"))
    } | Select-Object -First 1
    if ($ExistingBroker) {
        Set-Content -Path (Join-Path $Logs "kafka-local.pid") -Value $ExistingBroker.ProcessId -Encoding ASCII
    }
    Write-Host "Kafka ya responde en localhost:9092."
    exit 0
}

if (-not (Test-Path (Join-Path $KafkaHome "bin\windows\kafka-server-start.bat"))) {
    if (-not (Get-Command tar.exe -ErrorAction SilentlyContinue)) {
        throw "Windows no tiene tar.exe; no se puede extraer Kafka automaticamente."
    }
    Write-Host "Descargando Apache Kafka $Version (aprox. 116 MB, solo la primera vez)..."
    Invoke-WebRequest -UseBasicParsing -Uri $Url -OutFile $Archive
    $ActualSha512 = (Get-FileHash -Path $Archive -Algorithm SHA512).Hash
    if ($ActualSha512 -ne $ExpectedSha512) {
        throw "La descarga de Kafka no paso la verificacion SHA-512."
    }
    Write-Host "Extrayendo Kafka en $Base ..."
    & tar.exe -xzf $Archive -C $Base
    if ($LASTEXITCODE -ne 0) { throw "No se pudo extraer Kafka." }
    Remove-Item -LiteralPath $Archive -Force
}

$DataDir = (Join-Path $Base "data").Replace("\", "/")
$Config = Join-Path $Base "obelie-server.properties"
$Properties = @"
process.roles=broker,controller
node.id=1
controller.quorum.voters=1@127.0.0.1:9093
listeners=PLAINTEXT://127.0.0.1:9092,CONTROLLER://127.0.0.1:9093
advertised.listeners=PLAINTEXT://127.0.0.1:9092
listener.security.protocol.map=CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT
controller.listener.names=CONTROLLER
inter.broker.listener.name=PLAINTEXT
log.dirs=$DataDir
num.partitions=3
offsets.topic.replication.factor=1
transaction.state.log.replication.factor=1
transaction.state.log.min.isr=1
group.initial.rebalance.delay.ms=0
auto.create.topics.enable=false
"@
Set-Content -Path $Config -Value $Properties -Encoding ASCII

$Java = (Get-Command java).Source
$Classpath = Join-Path $KafkaHome "libs\*"
$KafkaLogDir = (Join-Path $Base "logs").Replace("\", "/")
$Log4j = (Join-Path $KafkaHome "config\log4j.properties").Replace("\", "/")
New-Item -ItemType Directory -Force -Path (Join-Path $Base "logs") | Out-Null
$Meta = Join-Path $Base "data\meta.properties"
if (-not (Test-Path $Meta)) {
    Write-Host "Inicializando almacenamiento KRaft..."
    $ClusterId = (& $Java "-Dlog4j.configuration=file:$Log4j" "-Dkafka.logs.dir=$KafkaLogDir" -cp $Classpath kafka.tools.StorageTool random-uuid | Select-Object -Last 1).Trim()
    if ($LASTEXITCODE -ne 0 -or -not $ClusterId) { throw "No se pudo generar el cluster ID de Kafka." }
    & $Java "-Dlog4j.configuration=file:$Log4j" "-Dkafka.logs.dir=$KafkaLogDir" -cp $Classpath kafka.tools.StorageTool format -t $ClusterId -c $Config
    if ($LASTEXITCODE -ne 0) { throw "No se pudo formatear el almacenamiento KRaft." }
}

$Out = Join-Path $Logs "kafka-local.out.log"
$Err = Join-Path $Logs "kafka-local.err.log"
$JavaArgs = "-Xms256M -Xmx512M `"-Dlog4j.configuration=file:$Log4j`" `"-Dkafka.logs.dir=$KafkaLogDir`" -cp `"$Classpath`" kafka.Kafka `"$Config`""
$Launcher = Start-Process -FilePath $Java -ArgumentList $JavaArgs `
    -WorkingDirectory $KafkaHome -RedirectStandardOutput $Out -RedirectStandardError $Err `
    -WindowStyle Hidden -PassThru

foreach ($intento in 1..90) {
    Start-Sleep -Seconds 1
    if (Test-KafkaPort) { break }
    if ($Launcher.HasExited) {
        throw "Kafka local termino durante el arranque. Revisa logs\kafka-local.err.log."
    }
}
if (-not (Test-KafkaPort)) {
    Stop-Process -Id $Launcher.Id -Force -ErrorAction SilentlyContinue
    throw "Kafka local no abrio localhost:9092. Revisa logs\kafka-local.err.log."
}

$KafkaPid = $Launcher.Id
Set-Content -Path (Join-Path $Logs "kafka-local.pid") -Value $KafkaPid -Encoding ASCII
Write-Host ("Kafka local iniciado. PID={0}" -f $KafkaPid)
