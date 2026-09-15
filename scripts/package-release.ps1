param(
  [Parameter(Mandatory = $true)][string]$Version,
  [Parameter(Mandatory = $true)][string]$OutputDirectory
)

$ErrorActionPreference = "Stop"
$root = Split-Path -Parent $PSScriptRoot
$binaryName = if ($IsWindows) { "kakune.exe" } else { "kakune" }
$binary = Join-Path $root "target/release/$binaryName"
if (-not (Test-Path -LiteralPath $binary)) {
  throw "Release binary was not found at $binary. Build kakune before packaging."
}

$platform = if ($IsWindows) {
  "x86_64-pc-windows-msvc"
} elseif ($IsMacOS -and [System.Runtime.InteropServices.RuntimeInformation]::OSArchitecture -eq "Arm64") {
  "aarch64-apple-darwin"
} elseif ($IsMacOS) {
  "x86_64-apple-darwin"
} else {
  "x86_64-unknown-linux-gnu"
}

New-Item -ItemType Directory -Force -Path $OutputDirectory | Out-Null
$name = "kakune-core-$Version-$platform"
$stage = Join-Path ([System.IO.Path]::GetTempPath()) "$name-$PID"
Remove-Item -Recurse -Force -ErrorAction SilentlyContinue $stage
New-Item -ItemType Directory -Force -Path $stage | Out-Null
try {
  Copy-Item -LiteralPath $binary -Destination (Join-Path $stage $binaryName)
  Copy-Item -LiteralPath (Join-Path $root "LICENSE") -Destination $stage
  Copy-Item -LiteralPath (Join-Path $root "README.md") -Destination $stage
  Copy-Item -LiteralPath (Join-Path $root "README.es.md") -Destination $stage
  Copy-Item -LiteralPath (Join-Path $root "examples") -Destination (Join-Path $stage "examples") -Recurse
  Copy-Item -LiteralPath (Join-Path $root "packaging") -Destination (Join-Path $stage "packaging") -Recurse

  if ($IsWindows) {
    $archive = Join-Path $OutputDirectory "$name.zip"
    Compress-Archive -Path (Join-Path $stage "*") -DestinationPath $archive -Force
  } else {
    $archive = Join-Path $OutputDirectory "$name.tar.gz"
    & tar -C $stage -czf $archive .
    if ($LASTEXITCODE -ne 0) { throw "tar failed with exit code $LASTEXITCODE" }
  }
  Write-Output $archive
} finally {
  Remove-Item -Recurse -Force -ErrorAction SilentlyContinue $stage
}
