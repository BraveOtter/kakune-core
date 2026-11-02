param(
  [Parameter(Mandatory = $true)][string]$Version,
  [Parameter(Mandatory = $true)][string]$OutputDirectory,
  [string]$Platform = "",
  [string[]]$Runner = @()
)

$ErrorActionPreference = "Stop"
$root = Split-Path -Parent $PSScriptRoot

$supportedPlatforms = @(
  "x86_64-pc-windows-msvc",
  "x86_64-unknown-linux-gnu",
  "x86_64-unknown-linux-musl",
  "aarch64-apple-darwin",
  "x86_64-apple-darwin"
)
if ($Platform -and $supportedPlatforms -notcontains $Platform) {
  throw "Unsupported release platform $Platform. Supported platforms: $($supportedPlatforms -join ', ')"
}
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
$isWindowsPlatform = $resolvedPlatform -eq "x86_64-pc-windows-msvc"
$isLinuxPlatform = $resolvedPlatform -like "x86_64-unknown-linux-*"
$isMacPlatform = $resolvedPlatform -like "*-apple-darwin"

$binaryName = if ($isWindowsPlatform) { "kakune.exe" } else { "kakune" }
$binary = Join-Path $root "target/release/$binaryName"
if (-not (Test-Path -LiteralPath $binary)) {
  throw "Release binary was not found at $binary. Build kakune before packaging."
}
$observationHelper = Join-Path $root "target/release/kakune-observation-helper"
if (($isLinuxPlatform -or $isMacPlatform) -and -not (Test-Path -LiteralPath $observationHelper -PathType Leaf)) {
  throw "Observation helper was not found at $observationHelper. Build it before packaging."
}
$pluginSandbox = Join-Path $root "target/release/kakune-plugin-sandbox"
if ($isLinuxPlatform -and -not (Test-Path -LiteralPath $pluginSandbox -PathType Leaf)) {
  throw "Experimental plugin launcher was not found at $pluginSandbox. Build it before packaging."
}

$platform = $resolvedPlatform

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

New-Item -ItemType Directory -Force -Path $OutputDirectory | Out-Null
$name = "kakune-core-$Version-$platform"
$stage = Join-Path ([System.IO.Path]::GetTempPath()) "$name-$PID"
Remove-Item -Recurse -Force -ErrorAction SilentlyContinue $stage
New-Item -ItemType Directory -Force -Path $stage | Out-Null
try {
  Copy-Item -LiteralPath $binary -Destination (Join-Path $stage $binaryName)
  if ($isLinuxPlatform -or $isMacPlatform) {
    Copy-Item -LiteralPath $observationHelper -Destination (Join-Path $stage "kakune-observation-helper")
  }
  if ($isLinuxPlatform) {
    Copy-Item -LiteralPath $pluginSandbox -Destination (Join-Path $stage "kakune-plugin-sandbox")
  }
  Copy-Item -LiteralPath (Join-Path $root "LICENSE") -Destination $stage
  Copy-Item -LiteralPath (Join-Path $root "README.md") -Destination $stage
  Copy-Item -LiteralPath (Join-Path $root "README.es.md") -Destination $stage
  Copy-Item -LiteralPath (Join-Path $root "examples") -Destination (Join-Path $stage "examples") -Recurse
  Copy-Item -LiteralPath (Join-Path $root "packaging") -Destination (Join-Path $stage "packaging") -Recurse

  if ($isWindowsPlatform) {
    $archive = Join-Path $OutputDirectory "$name.zip"
    Compress-Archive -Path (Join-Path $stage "*") -DestinationPath $archive -Force
  } else {
    $archive = Join-Path $OutputDirectory "$name.tar.gz"
    if ($Runner.Count -eq 0) {
      & tar -C $stage -czf $archive .
      if ($LASTEXITCODE -ne 0) { throw "tar failed with exit code $LASTEXITCODE" }
    } else {
      # A Unix archive must keep the executable bit on its launchers. The host
      # filesystem cannot express it, so the archive is written where POSIX
      # modes exist and the staged files are normalized there.
      $posixStage = ConvertTo-RunnerPath -Path $stage
      $posixArchive = ConvertTo-RunnerPath -Path $archive
      $executables = @("$posixStage/$binaryName")
      if ($isLinuxPlatform -or $isMacPlatform) {
        $executables += "$posixStage/kakune-observation-helper"
      }
      if ($isLinuxPlatform) {
        $executables += "$posixStage/kakune-plugin-sandbox"
      }
      $quoted = $executables | ForEach-Object { "'$_'" }
      $script = "find '$posixStage' -type d -exec chmod 755 {} + ; " +
        "find '$posixStage' -type f -exec chmod 644 {} + ; " +
        "chmod 755 $($quoted -join ' ') ; " +
        "tar -C '$posixStage' -czf '$posixArchive' ."
      if ($Runner.Count -eq 1) {
        & $Runner[0] /bin/sh -c $script
      } else {
        & $Runner[0] @($Runner[1..($Runner.Count - 1)]) /bin/sh -c $script
      }
      if ($LASTEXITCODE -ne 0) { throw "tar failed with exit code $LASTEXITCODE" }
    }
  }
  Write-Output $archive
} finally {
  Remove-Item -Recurse -Force -ErrorAction SilentlyContinue $stage
}
