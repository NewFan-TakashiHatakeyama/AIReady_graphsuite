param(
    [Parameter(Mandatory = $false)]
    [string]$OutputZip = "layers/common-layer/common-layer.zip",
    [Parameter(Mandatory = $false)]
    [string]$PythonVersion = "3.12"
)

$ErrorActionPreference = "Stop"

$root = Split-Path -Parent $PSScriptRoot
$requirementsPath = Join-Path $root "layers\common-layer\requirements.txt"
$buildRoot = Join-Path $root "layers\common-layer\build"
$pythonTarget = Join-Path $buildRoot "python"
$outputZipPath = Join-Path $root $OutputZip

if (-not (Test-Path $requirementsPath)) {
    throw "requirements.txt was not found: $requirementsPath"
}

Write-Host "[common-layer] Python version target: $PythonVersion"
Write-Host "[common-layer] Cleaning previous build artifacts..."
if (Test-Path $buildRoot) {
    Remove-Item -Recurse -Force $buildRoot
}
New-Item -ItemType Directory -Path $pythonTarget -Force | Out-Null

Write-Host "[common-layer] Installing dependencies..."
& python -m pip install -r $requirementsPath -t $pythonTarget
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

Write-Host "[common-layer] Creating zip package..."
Compress-Archive -Path (Join-Path $buildRoot "*") -DestinationPath $outputZipPath -Force

Write-Host "[common-layer] Build complete: $outputZipPath"
Write-Host "[common-layer] Verify import example: python -c ""import openlineage; print('ok')"""
