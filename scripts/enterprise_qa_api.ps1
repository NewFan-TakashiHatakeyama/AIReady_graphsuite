#Requires -Version 5.1
<#
.SYNOPSIS
  Enterprise QA L1: API 単体テスト（Connect 削除・Governance 契約を含む全 api/tests）。
.NOTES
  リポジトリルートから: .\scripts\enterprise_qa_api.ps1
  CI では .github/workflows/ci-api.yml と同等の `pytest tests/` を実行する。
#>
$ErrorActionPreference = 'Stop'
$Root = Split-Path -Parent (Split-Path -Parent $MyInvocation.MyCommand.Path)
$ApiDir = Join-Path $Root 'api'
Set-Location $ApiDir
python -m pytest tests/ -q --tb=short
