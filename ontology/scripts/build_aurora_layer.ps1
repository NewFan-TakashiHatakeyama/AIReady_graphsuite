param(
    [Parameter(Mandatory = $false)]
    [string]$OutputZip = "layers/aurora-layer/aurora-layer.zip",
    [Parameter(Mandatory = $false)]
    [string]$PythonVersion = "3.12"
)

$ErrorActionPreference = "Stop"

$root = Split-Path -Parent $PSScriptRoot
$requirementsPath = Join-Path $root "layers\aurora-layer\requirements.txt"
$buildRoot = Join-Path $root "layers\aurora-layer\build"
$pythonTarget = Join-Path $buildRoot "python"
$outputZipPath = Join-Path $root $OutputZip

if (-not (Test-Path $requirementsPath)) {
    throw "requirements.txt was not found: $requirementsPath"
}

Write-Host "[aurora-layer] Python version target: $PythonVersion"
Write-Host "[aurora-layer] Cleaning previous build artifacts..."
if (Test-Path $buildRoot) {
    Remove-Item -Recurse -Force $buildRoot
}
New-Item -ItemType Directory -Path $pythonTarget -Force | Out-Null

Write-Host "[aurora-layer] Installing Lambda-compatible dependencies..."
& python -m pip install `
    --platform manylinux2014_x86_64 `
    --implementation cp `
    --python-version $PythonVersion `
    --only-binary=:all: `
    --upgrade `
    -r $requirementsPath `
    -t $pythonTarget
if ($LASTEXITCODE -ne 0) {
    throw "pip install failed."
}

$zipDir = Split-Path -Parent $outputZipPath
if (-not (Test-Path $zipDir)) {
    New-Item -ItemType Directory -Path $zipDir -Force | Out-Null
}

if (Test-Path $outputZipPath) {
    Remove-Item -Force $outputZipPath
}

Write-Host "[aurora-layer] Creating zip package..."
Compress-Archive -Path (Join-Path $buildRoot "*") -DestinationPath $outputZipPath -Force

Write-Host "[aurora-layer] Build complete: $outputZipPath"
Write-Host "[aurora-layer] Next step: attach this layer to Lambda and verify import psycopg2 in Lambda runtime."
