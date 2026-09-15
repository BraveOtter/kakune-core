param([Parameter(Mandatory = $true)][string]$AssetPath)

$ErrorActionPreference = "Stop"
$destination = Join-Path ([System.IO.Path]::GetTempPath()) "kakune-core-smoke-$PID"
$data = Join-Path $destination "data"
New-Item -ItemType Directory -Force -Path $destination | Out-Null
try {
  if ($AssetPath.EndsWith(".zip")) {
    Expand-Archive -LiteralPath $AssetPath -DestinationPath $destination -Force
  } else {
    & tar -C $destination -xzf $AssetPath
    if ($LASTEXITCODE -ne 0) { throw "tar failed with exit code $LASTEXITCODE" }
  }
  $binaryName = if ($IsWindows) { "kakune.exe" } else { "kakune" }
  $binary = (Get-ChildItem -Path $destination -Filter $binaryName -File -Recurse | Select-Object -First 1).FullName
  if (-not $binary) { throw "The packaged archive does not contain $binaryName" }
  $example = (Get-ChildItem -Path $destination -Filter "write-note.kakune.yaml" -File -Recurse | Select-Object -First 1).FullName
  & $binary --version
  if ($LASTEXITCODE -ne 0) { throw "Version command failed with exit code $LASTEXITCODE" }
  & $binary init --data-dir $data
  if ($LASTEXITCODE -ne 0) { throw "Initialization failed with exit code $LASTEXITCODE" }
  & $binary run $example --data-dir $data
  if ($LASTEXITCODE -ne 0) { throw "Workflow smoke failed with exit code $LASTEXITCODE" }
  if (-not (Test-Path -LiteralPath (Join-Path $data "workspace/notes/hello.txt"))) {
    throw "The packaged binary did not create the expected workflow output"
  }
} finally {
  Remove-Item -Recurse -Force -ErrorAction SilentlyContinue $destination
}
