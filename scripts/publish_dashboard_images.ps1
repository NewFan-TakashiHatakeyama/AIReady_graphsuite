#Requires -Version 5.1
<#
.SYNOPSIS
  Repo root から API + WebUI を ECR に push し ECS を再デプロイする（ラッパー）。

.DESCRIPTION
  実装は back\scripts\publish_dashboard_images.ps1 を呼び出します。

.NOTES
  Docker Desktop を起動してから実行してください。
  例: .\scripts\publish_dashboard_images.ps1 -Region ap-northeast-1
#>
param(
    [string] $Region = "ap-northeast-1",
    [string] $ViteBackendUrl = "",
    [switch] $SkipEcsRollout,
    [string] $EcsCluster = "aiready-dashboard",
    [string] $EcsApiService = "aiready-dashboard-api",
    [string] $EcsWebService = "aiready-dashboard-web"
)

$ErrorActionPreference = "Stop"
$impl = Join-Path $PSScriptRoot "..\back\scripts\publish_dashboard_images.ps1"
if (-not (Test-Path $impl)) {
    throw "Missing $impl"
}
& $impl `
    -Region $Region `
    -ViteBackendUrl $ViteBackendUrl `
    -SkipEcsRollout:$SkipEcsRollout `
    -EcsCluster $EcsCluster `
    -EcsApiService $EcsApiService `
    -EcsWebService $EcsWebService
