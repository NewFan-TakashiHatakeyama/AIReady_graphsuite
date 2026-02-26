param(
    [Parameter(Mandatory = $true)]
    [ValidateSet("dev", "stg", "prod")]
    [string]$Environment,
    [Parameter(Mandatory = $false)]
    [switch]$RequireApprovalNever,
    [Parameter(Mandatory = $false)]
    [switch]$SeedSsm,
    [Parameter(Mandatory = $false)]
    [string]$PiiEncryptionKey
)

$ErrorActionPreference = "Stop"

if ($SeedSsm -and [string]::IsNullOrWhiteSpace($PiiEncryptionKey)) {
    throw "When using -SeedSsm, -PiiEncryptionKey is required."
}

$root = Split-Path -Parent $PSScriptRoot
$configPath = Join-Path $root "cdk\environments.json"
$config = Get-Content -Raw -Path $configPath | ConvertFrom-Json
$envConfig = $config.$Environment

if ($null -eq $envConfig) {
    throw "Environment '$Environment' is not defined in environments.json."
}

$tenantId = $envConfig.tenantId

if ($SeedSsm) {
    $seedScript = Join-Path $root "scripts\seed_ssm_parameters.ps1"
    & $seedScript -TenantId $tenantId -PiiEncryptionKey $PiiEncryptionKey
}

$deployArgs = @("deploy", "--all", "-c", "env=$Environment")
if ($RequireApprovalNever) {
    $deployArgs += @("--require-approval", "never")
}

Push-Location $root
try {
    & cdk @deployArgs
    if ($LASTEXITCODE -ne 0) {
        throw "cdk deploy failed."
    }
}
finally {
    Pop-Location
}
