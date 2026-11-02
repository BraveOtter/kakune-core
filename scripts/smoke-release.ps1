param(
  [Parameter(Mandatory = $true)][string]$AssetPath,
  [string[]]$Runner = @(),
  [string]$Platform = ""
)

$ErrorActionPreference = "Stop"
$destination = Join-Path ([System.IO.Path]::GetTempPath()) "kakune-core-smoke-$PID"
$data = Join-Path $destination "data"
New-Item -ItemType Directory -Force -Path $destination | Out-Null

$resolvedPlatform = if ($Platform) {
  $Platform
} elseif ($IsWindows) {
  "x86_64-pc-windows-msvc"
} elseif ($IsMacOS -and [System.Runtime.InteropServices.RuntimeInformation]::OSArchitecture -eq "Arm64") {
  "aarch64-apple-darwin"
} elseif ($IsMacOS) {
  "x86_64-apple-darwin"
} else {
  "x86_64-unknown-linux-gnu"
}

function ConvertTo-RunnerPath {
  param([string]$Path)
  if ($Runner.Count -eq 0) { return $Path }
  if ($Path -match '^([A-Za-z]):\\(.*)$') {
    $drive = $Matches[1].ToLowerInvariant()
    $rest = $Matches[2] -replace '\\', '/'
    return "/mnt/$drive/$rest"
  }
  return $Path
}

function Invoke-Packaged {
  param([string]$Executable, [string[]]$Arguments)
  $resolved = ConvertTo-RunnerPath -Path $Executable
  $converted = @($Arguments | ForEach-Object { ConvertTo-RunnerPath -Path $_ })
  if ($Runner.Count -eq 0) {
    & $resolved @converted
    return
  }
  if ($Runner.Count -eq 1) {
    & $Runner[0] $resolved @converted
    return
  }
  & $Runner[0] @($Runner[1..($Runner.Count - 1)]) $resolved @converted
}

try {
  if ($AssetPath.EndsWith(".zip")) {
    Expand-Archive -LiteralPath $AssetPath -DestinationPath $destination -Force
  } else {
    & tar -C $destination -xzf $AssetPath
    if ($LASTEXITCODE -ne 0) { throw "tar failed with exit code $LASTEXITCODE" }
  }
  $binaryName = if ($resolvedPlatform -eq "x86_64-pc-windows-msvc") { "kakune.exe" } else { "kakune" }
  $binary = (Get-ChildItem -Path $destination -Filter $binaryName -File -Recurse | Select-Object -First 1).FullName
  if (-not $binary) { throw "The packaged archive does not contain $binaryName" }
  $sibling = Split-Path -Parent $binary
  $observationHelper = Join-Path $sibling "kakune-observation-helper"
  $pluginSandbox = Join-Path $sibling "kakune-plugin-sandbox"
  if (Test-Path -LiteralPath $observationHelper -PathType Leaf) {
    $response = Invoke-Packaged -Executable $observationHelper -Arguments @((Join-Path $destination "absent.sqlite3")) | ConvertFrom-Json
    if ($LASTEXITCODE -ne 0 -or $response.status -ne "unavailable") {
      throw "The packaged observation helper failed its smoke test"
    }
  }
  if (Test-Path -LiteralPath $pluginSandbox -PathType Leaf) {
    $marker = Join-Path $destination "launcher-marker"
    Invoke-Packaged -Executable $pluginSandbox -Arguments @("/bin/touch", $marker) | Out-Null
    if (Test-Path -LiteralPath $marker) {
      throw "The packaged plugin launcher ran the command without filesystem restrictions"
    }
  }
  $example = (Get-ChildItem -Path $destination -Filter "write-note.kakune.yaml" -File -Recurse | Select-Object -First 1).FullName
  Invoke-Packaged -Executable $binary -Arguments @("--version")
  if ($LASTEXITCODE -ne 0) { throw "Version command failed with exit code $LASTEXITCODE" }
  Invoke-Packaged -Executable $binary -Arguments @("init", "--data-dir", $data)
  if ($LASTEXITCODE -ne 0) { throw "Initialization failed with exit code $LASTEXITCODE" }
  Invoke-Packaged -Executable $binary -Arguments @("--standalone", "run", $example, "--data-dir", $data)
  if ($LASTEXITCODE -ne 0) { throw "Workflow smoke failed with exit code $LASTEXITCODE" }
  if (-not (Test-Path -LiteralPath (Join-Path $data "workspace/notes/hello.txt"))) {
    throw "The packaged binary did not create the expected workflow output"
  }
} finally {
  Remove-Item -Recurse -Force -ErrorAction SilentlyContinue $destination
}
