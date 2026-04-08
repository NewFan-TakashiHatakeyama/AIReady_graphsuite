#Requires -Version 5.1
<#
.SYNOPSIS
  remediateFinding Lambda の CloudWatch Logs を調査する（是正 incomplete / Graph 失敗の切り分け）。

.DESCRIPTION
  filter-log-events で execute_remediation、DELETE 失敗、manual_required 等を検索します。
  AWS CLI v2 と適切なプロファイル/リージョンが必要です。

.PARAMETER LogGroupName
  例: /aws/lambda/AIReadyGov-remediateFinding（環境に合わせて変更）

.PARAMETER FilterPattern
  CloudWatch の filter-pattern（単語に部分一致）。例: execute_remediation / Delete permission / manual_required
  別キーワードで再実行して突き合わせてください。

.NOTES
  リポジトリルートから:
    .\scripts\governance_remediation_cloudwatch.ps1 -Region ap-northeast-1
    .\scripts\governance_remediation_cloudwatch.ps1 -FilterPattern "manual_required" -Minutes 120

  GOVERNANCE_REMEDIATE_FINDING_LAMBDA_NAME と実際のロググループ名を一致させてください。
#>
param(
    [string]$Region = $(if ($env:AWS_REGION) { $env:AWS_REGION } elseif ($env:AWS_DEFAULT_REGION) { $env:AWS_DEFAULT_REGION } else { 'ap-northeast-1' }),
    [string]$LogGroupName = '/aws/lambda/AIReadyGov-remediateFinding',
    [string]$FilterPattern = 'execute_remediation',
    [int]$Minutes = 60,
    [int]$Limit = 100
)

$ErrorActionPreference = 'Stop'

$start = [int64](([DateTimeOffset]::UtcNow.AddMinutes(-$Minutes)).ToUnixTimeMilliseconds())
$end = [int64](([DateTimeOffset]::UtcNow).ToUnixTimeMilliseconds())

Write-Host "[governance-cw] LogGroup=$LogGroupName Region=$Region Window=last ${Minutes}m"
Write-Host "[governance-cw] FilterPattern=$FilterPattern"

aws logs filter-log-events `
    --log-group-name $LogGroupName `
    --filter-pattern $FilterPattern `
    --start-time $start `
    --end-time $end `
    --region $Region `
    --limit $Limit `
    --output json

if ($LASTEXITCODE -ne 0) {
    Write-Error "aws logs filter-log-events failed (exit=$LASTEXITCODE). Check AWS CLI, profile, and log group name."
}
