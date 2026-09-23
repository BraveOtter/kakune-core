[CmdletBinding()]
param(
  [string]$Version = "latest",
  [string]$InstallDir,
  [switch]$NoPathUpdate
)

$ErrorActionPreference = "Stop"
$ProgressPreference = "SilentlyContinue"
$repository = "BraveOtter/kakune-core"

if ($env:OS -ne "Windows_NT") {
  throw "install.ps1 must be run on Windows."
}
if ($env:PROCESSOR_ARCHITEW6432) {
  $architecture = $env:PROCESSOR_ARCHITEW6432
} else {
  $architecture = $env:PROCESSOR_ARCHITECTURE
}
if ($architecture -ne "AMD64") {
  throw "Kakune currently publishes a Windows x86_64 build; this system reports '$architecture'."
}

if ([string]::IsNullOrWhiteSpace($InstallDir)) {
  if ([string]::IsNullOrWhiteSpace($env:LOCALAPPDATA)) {
    $InstallDir = Join-Path $HOME ".local\bin"
  } else {
    $InstallDir = Join-Path $env:LOCALAPPDATA "Programs\Kakune"
  }
}
$InstallDir = [System.IO.Path]::GetFullPath($InstallDir)
$binaryPath = Join-Path $InstallDir "kakune.exe"

if ($Version -eq "latest") {
  $releaseUri = "https://api.github.com/repos/$repository/releases/latest"
} else {
  $requestedVersion = $Version
  if ($requestedVersion.StartsWith("v", [System.StringComparison]::OrdinalIgnoreCase)) {
    $requestedVersion = $requestedVersion.Substring(1)
  }
  if ($requestedVersion -notmatch '^\d+\.\d+\.\d+(?:-[0-9A-Za-z.-]+)?(?:\+[0-9A-Za-z.-]+)?$') {
    throw "Invalid version '$Version'. Use a version such as 1.2.3 or 'latest'."
  }
  $releaseUri = "https://api.github.com/repos/$repository/releases/tags/v$requestedVersion"
}

$temporaryDirectory = Join-Path ([System.IO.Path]::GetTempPath()) ("kakune-install-" + [guid]::NewGuid().ToString("N"))
$stagedBinary = $null
$serviceStopped = $false
New-Item -ItemType Directory -Force -Path $temporaryDirectory | Out-Null

try {
  $headers = @{
    Accept = "application/vnd.github+json"
    "User-Agent" = "Kakune-Core-Installer"
  }
  $release = Invoke-RestMethod -Uri $releaseUri -Headers $headers
  if ($release.tag_name -notmatch '^v(?<version>\d+\.\d+\.\d+(?:-[0-9A-Za-z.-]+)?(?:\+[0-9A-Za-z.-]+)?)$') {
    throw "GitHub returned an unexpected release tag '$($release.tag_name)'."
  }
  $releaseVersion = $Matches.version
  $archiveName = "kakune-core-$releaseVersion-x86_64-pc-windows-msvc.zip"
  $archiveAsset = $release.assets | Where-Object { $_.name -eq $archiveName } | Select-Object -First 1
  $checksumAsset = $release.assets | Where-Object { $_.name -eq "SHA256SUMS" } | Select-Object -First 1
  if (-not $archiveAsset) {
    throw "Release $($release.tag_name) does not contain the Windows asset '$archiveName'."
  }
  if (-not $checksumAsset) {
    throw "Release $($release.tag_name) does not contain SHA256SUMS."
  }

  $archivePath = Join-Path $temporaryDirectory $archiveName
  $checksumPath = Join-Path $temporaryDirectory "SHA256SUMS"
  Invoke-WebRequest -Uri $archiveAsset.browser_download_url -OutFile $archivePath
  Invoke-WebRequest -Uri $checksumAsset.browser_download_url -OutFile $checksumPath

  $expectedHash = $null
  foreach ($line in Get-Content -LiteralPath $checksumPath) {
    if ($line -match '^(?<hash>[0-9a-fA-F]{64})\s+\*?(?<name>.+?)\s*$' -and $Matches.name -eq $archiveName) {
      $expectedHash = $Matches.hash.ToLowerInvariant()
      break
    }
  }
  if (-not $expectedHash) {
    throw "SHA256SUMS does not contain an entry for '$archiveName'."
  }
  $actualHash = (Get-FileHash -LiteralPath $archivePath -Algorithm SHA256).Hash.ToLowerInvariant()
  if ($actualHash -ne $expectedHash) {
    throw "SHA-256 verification failed for '$archiveName'."
  }

  $extractDirectory = Join-Path $temporaryDirectory "extracted"
  Expand-Archive -LiteralPath $archivePath -DestinationPath $extractDirectory
  $packagedBinary = Get-ChildItem -LiteralPath $extractDirectory -Filter "kakune.exe" -File -Recurse | Select-Object -First 1
  if (-not $packagedBinary) {
    throw "The verified archive does not contain kakune.exe."
  }

  New-Item -ItemType Directory -Force -Path $InstallDir | Out-Null
  $stagedBinary = Join-Path $InstallDir ("kakune.exe.new-" + [guid]::NewGuid().ToString("N"))
  [System.IO.File]::Copy($packagedBinary.FullName, $stagedBinary, $true)

  # Stop and restart the Kakune service only when it uses this exact executable.
  try {
    $service = Get-CimInstance -ClassName Win32_Service -Filter "Name='KakuneCore'" -ErrorAction SilentlyContinue
    if ($service -and $service.State -eq "Running") {
      $serviceExecutable = $null
      if ($service.PathName -match '^\s*"(?<path>[^"]+\.exe)"') {
        $serviceExecutable = $Matches.path
      } elseif ($service.PathName -match '^\s*(?<path>.+?\.exe)(?:\s|$)') {
        $serviceExecutable = $Matches.path
      }
      if ($serviceExecutable -and [System.IO.Path]::GetFullPath($serviceExecutable) -ieq $binaryPath) {
        Write-Host "Stopping KakuneCore service for upgrade..."
        try {
          Stop-Service -Name "KakuneCore" -ErrorAction Stop
        } catch {
          throw "Could not stop KakuneCore. Run this installer from an elevated PowerShell session to update the running service. $($_.Exception.Message)"
        }
        $serviceStopped = $true
        (Get-Service -Name "KakuneCore").WaitForStatus(
          [System.ServiceProcess.ServiceControllerStatus]::Stopped,
          [TimeSpan]::FromSeconds(30)
        )
      }
    }

    if (Test-Path -LiteralPath $binaryPath) {
      try {
        [System.IO.File]::Replace($stagedBinary, $binaryPath, $null)
      } catch {
        throw "Could not replace '$binaryPath'. Stop any foreground Kakune process and retry. $($_.Exception.Message)"
      }
    } else {
      [System.IO.File]::Move($stagedBinary, $binaryPath)
    }
    $stagedBinary = $null
  } finally {
    if ($serviceStopped) {
      Write-Host "Starting KakuneCore service..."
      try {
        Start-Service -Name "KakuneCore" -ErrorAction Stop
        (Get-Service -Name "KakuneCore").WaitForStatus(
          [System.ServiceProcess.ServiceControllerStatus]::Running,
          [TimeSpan]::FromSeconds(30)
        )
      } catch {
        throw "Kakune was updated, but KakuneCore could not be restarted. Start it manually with 'kakune service start'. $($_.Exception.Message)"
      }
    }
  }

  if (-not $NoPathUpdate) {
    $userPath = [System.Environment]::GetEnvironmentVariable("Path", "User")
    $pathEntries = @($userPath -split ";" | Where-Object { -not [string]::IsNullOrWhiteSpace($_) })
    $alreadyInUserPath = $pathEntries | Where-Object { $_.TrimEnd("\") -ieq $InstallDir.TrimEnd("\") }
    if (-not $alreadyInUserPath) {
      $newUserPath = if ([string]::IsNullOrWhiteSpace($userPath)) { $InstallDir } else { "$userPath;$InstallDir" }
      [System.Environment]::SetEnvironmentVariable("Path", $newUserPath, "User")
    }

    $processPathEntries = @($env:Path -split ";")
    $alreadyInProcessPath = $processPathEntries | Where-Object { $_.TrimEnd("\") -ieq $InstallDir.TrimEnd("\") }
    if (-not $alreadyInProcessPath) {
      $env:Path = if ([string]::IsNullOrWhiteSpace($env:Path)) { $InstallDir } else { "$env:Path;$InstallDir" }
    }
  }

  Write-Host "Installed Kakune $($release.tag_name) at $binaryPath"
  Write-Host "Run 'kakune init' to initialize Kakune Core."
  if ($NoPathUpdate) {
    Write-Host "Add '$InstallDir' to your PATH to run 'kakune' without its full path."
  } else {
    Write-Host "If this terminal cannot find 'kakune', open a new terminal to reload PATH."
  }
} finally {
  if ($stagedBinary -and (Test-Path -LiteralPath $stagedBinary)) {
    Remove-Item -LiteralPath $stagedBinary -Force -ErrorAction SilentlyContinue
  }
  Remove-Item -LiteralPath $temporaryDirectory -Recurse -Force -ErrorAction SilentlyContinue
}
