param(
    [Parameter(Mandatory = $true)]
    [string]$HostName,
    [Parameter(Mandatory = $false)]
    [int]$Port = 5432,
    [Parameter(Mandatory = $false)]
    [string]$Database = "ai_ready_ontology",
    [Parameter(Mandatory = $false)]
    [string]$UserName = "ontology_app",
    [Parameter(Mandatory = $true)]
    [string]$Password
)

$ErrorActionPreference = "Stop"

$scriptRoot = Split-Path -Parent $MyInvocation.MyCommand.Path
$migrationDir = Join-Path $scriptRoot "migrations"

if (-not (Get-Command psql -ErrorAction SilentlyContinue)) {
    throw "psql command not found. Install PostgreSQL client tools first."
}

$env:PGPASSWORD = $Password

try {
    $files = Get-ChildItem -Path $migrationDir -Filter "*.sql" | Sort-Object Name
    foreach ($file in $files) {
        Write-Host "Applying migration: $($file.Name)"
        & psql `
            --host $HostName `
            --port $Port `
            --dbname $Database `
            --username $UserName `
            --file $file.FullName `
            --set ON_ERROR_STOP=on

        if ($LASTEXITCODE -ne 0) {
            throw "Migration failed: $($file.Name)"
        }
    }
}
finally {
    Remove-Item Env:PGPASSWORD -ErrorAction SilentlyContinue
}

Write-Host "All migrations completed successfully."
