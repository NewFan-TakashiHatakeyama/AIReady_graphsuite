#Requires -Version 5.1
<#
.SYNOPSIS
  analyzeExposure / schemaTransform の CloudWatch Logs を調査する（乖離調査・エラー・スキップの切り分け）。

.DESCRIPTION
  filter-log-events で直近ウィンドウのログを検索します。差分 item_id の時刻付近では -Minutes を広げ、
  FilterPattern を item_id / ERROR / Record processing 等に変えて突き合わせてください。

.PARAMETER Target
  analyzeExposure | schemaTransform | both（both は順に2回 aws を実行）

.NOTES
  リポジトリルートから:
    .\scripts\ontology_governance_stream_lambdas_cloudwatch.ps1 -Region ap-northeast-1 -Target analyzeExposure -FilterPattern "ERROR"
    .\scripts\ontology_governance_stream_lambdas_cloudwatch.ps1 -Target schemaTransform -FilterPattern "SchemaTransform" -Minutes 180

  ロググループ名は CDK の function_name に合わせています（変更時は -AnalyzeLogGroup / -SchemaLogGroup）。
#>
param(
    [ValidateSet('analyzeExposure', 'schemaTransform', 'both')]
    [string]$Target = 'both',
    [string]$Region = $(if ($env:AWS_REGION) { $env:AWS_REGION } elseif ($env:AWS_DEFAULT_REGION) { $env:AWS_DEFAULT_REGION } else { 'ap-northeast-1' }),
    [string]$AnalyzeLogGroup = '/aws/lambda/AIReadyGov-analyzeExposure',
    [string]$SchemaLogGroup = '/aws/lambda/AIReadyOntology-schemaTransform',
    [string]$FilterPattern = '',
    [int]$Minutes = 60,
    [int]$Limit = 100
)

$ErrorActionPreference = 'Stop'

function Invoke-FilterLogs {
    param(
        [string]$LogGroupName,
        [string]$Label
    )
    $start = [int64](([DateTimeOffset]::UtcNow.AddMinutes(-$Minutes)).ToUnixTimeMilliseconds())
    $end = [int64](([DateTimeOffset]::UtcNow).ToUnixTimeMilliseconds())
    Write-Host ""
    Write-Host "[$Label] LogGroup=$LogGroupName Region=$Region Window=last ${Minutes}m FilterPattern=$FilterPattern"
    $awsArgs = @(
        'logs', 'filter-log-events',
        '--log-group-name', $LogGroupName,
        '--start-time', "$start",
        '--end-time', "$end",
        '--region', $Region,
        '--limit', "$Limit",
        '--output', 'json'
    )
    if ($FilterPattern) {
        $awsArgs += @('--filter-pattern', $FilterPattern)
    }
    & aws @awsArgs
    if ($LASTEXITCODE -ne 0) {
        Write-Error "aws logs filter-log-events failed for $Label (exit=$LASTEXITCODE)."
    }
}

if ($Target -eq 'analyzeExposure' -or $Target -eq 'both') {
    Invoke-FilterLogs -LogGroupName $AnalyzeLogGroup -Label 'analyzeExposure'
}
if ($Target -eq 'schemaTransform' -or $Target -eq 'both') {
    Invoke-FilterLogs -LogGroupName $SchemaLogGroup -Label 'schemaTransform'
}
