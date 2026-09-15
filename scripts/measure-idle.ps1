param(
    [string]$Binary = (Join-Path $PSScriptRoot "..\target\release\kakune.exe"),
    [string]$DataDir = (Join-Path ([System.IO.Path]::GetTempPath()) ("kakune-idle-" + [guid]::NewGuid())),
    [ValidateRange(10, 3600)]
    [int]$SampleSeconds = 600,
    [ValidateRange(1, 60)]
    [int]$IntervalSeconds = 5,
    [ValidateRange(1, 2147483647)]
    [int64]$MaxPeakPrivateBytes = 268435456,
    [ValidateRange(0, 100)]
    [double]$MaxAverageCpuPercent = 1
)

$ErrorActionPreference = "Stop"

if (-not (Test-Path -LiteralPath $Binary -PathType Leaf)) {
    throw "Kakune binary not found: $Binary. Build it with cargo build --release first."
}

New-Item -ItemType Directory -Path $DataDir -Force | Out-Null
$logPath = Join-Path $DataDir "idle-measurement.log"
$errorLogPath = Join-Path $DataDir "idle-measurement-error.log"
$process = Start-Process -FilePath $Binary -ArgumentList @("daemon", "--data-dir", $DataDir, "--listen", "127.0.0.1:0") -RedirectStandardOutput $logPath -RedirectStandardError $errorLogPath -PassThru

try {
    Start-Sleep -Seconds 1
    if ($process.HasExited) {
        throw "Core stopped during idle startup. See $logPath"
    }

    $startCpu = (Get-Process -Id $process.Id).CPU
    $start = Get-Date
    $samples = @()
    while (((Get-Date) - $start).TotalSeconds -lt $SampleSeconds) {
        Start-Sleep -Seconds $IntervalSeconds
        $current = Get-Process -Id $process.Id -ErrorAction Stop
        $samples += $current.PrivateMemorySize64
    }
    $elapsed = ((Get-Date) - $start).TotalSeconds
    $endCpu = (Get-Process -Id $process.Id).CPU
    $children = Get-CimInstance Win32_Process | Where-Object { $_.ParentProcessId -eq $process.Id }
    $unexpectedRuntimes = $children | Where-Object { $_.Name -match "^(codex|node|python)(\.exe)?$" }
    if ($unexpectedRuntimes) {
        throw "Idle Core started AI runtime processes: $($unexpectedRuntimes.Name -join ', ')"
    }

    $measurement = [pscustomobject]@{
        pid = $process.Id
        sampleSeconds = [math]::Round($elapsed, 2)
        averagePrivateBytes = [math]::Round(($samples | Measure-Object -Average).Average)
        peakPrivateBytes = ($samples | Measure-Object -Maximum).Maximum
        averageCpuPercent = [math]::Round((($endCpu - $startCpu) / $elapsed / [Environment]::ProcessorCount) * 100, 3)
        aiRuntimeChildren = @($unexpectedRuntimes).Count
        stdoutLog = $logPath
        stderrLog = $errorLogPath
    }
    if ($measurement.peakPrivateBytes -gt $MaxPeakPrivateBytes) {
        throw "Idle Core peak private memory $($measurement.peakPrivateBytes) exceeds $MaxPeakPrivateBytes bytes"
    }
    if ($measurement.averageCpuPercent -gt $MaxAverageCpuPercent) {
        throw "Idle Core average CPU $($measurement.averageCpuPercent)% exceeds $MaxAverageCpuPercent%"
    }
    $measurement | ConvertTo-Json
}
finally {
    if (-not $process.HasExited) {
        Stop-Process -Id $process.Id -Force
    }
}
