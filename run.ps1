#Requires -Version 5.1
<#
.SYNOPSIS
  API (graphsuite_server) + WebUI (Vite) をローカルで起動する（run.sh の PowerShell 版）。

.NOTES
  環境変数: API_PORT, WEBUI_PORT, WEBUI_FALLBACK_PORTS, VITE_PROXY_TARGET
  停止: このウィンドウで Ctrl+C
#>
$ErrorActionPreference = 'Stop'

$RootDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$ApiDir = Join-Path $RootDir 'api'
$WebuiDir = Join-Path $RootDir 'webui'

$API_PORT = if ($env:API_PORT) { [int]$env:API_PORT } else { 9621 }
$WEBUI_PORT = if ($env:WEBUI_PORT) { [int]$env:WEBUI_PORT } else { 5173 }
$fallbackRaw = if ($env:WEBUI_FALLBACK_PORTS) { $env:WEBUI_FALLBACK_PORTS } else { '5174 5175' }
$WEBUI_FALLBACK_PORTS = @($fallbackRaw -split '\s+' | Where-Object { $_ })

function Free-TcpListenPort {
    param([int]$Port)
    if ($Port -le 0) { return }
    try {
        Get-NetTCPConnection -LocalPort $Port -State Listen -ErrorAction SilentlyContinue |
            ForEach-Object {
                $owning = [int]$_.OwningProcess
                if ($owning -gt 0) {
                    Stop-Process -Id $owning -Force -ErrorAction SilentlyContinue
                }
            }
    }
    catch {
        # ignore
    }
}

function Get-PythonLaunch {
    $pairs = @(
        @{ Exe = 'python'; Prefix = @() },
        @{ Exe = 'python3'; Prefix = @() },
        @{ Exe = 'py'; Prefix = @('-3') }
    )
    foreach ($p in $pairs) {
        $cmd = Get-Command $p.Exe -ErrorAction SilentlyContinue
        if ($cmd) {
            return @{ Exe = $cmd.Source; Prefix = $p.Prefix }
        }
    }
    return $null
}

function Test-ApiHealth {
    param(
        [string]$PythonExe,
        [string[]]$PythonPrefix,
        [int]$Port
    )
    $code = "import sys, urllib.request; urllib.request.urlopen('http://127.0.0.1:$Port/health', timeout=1); sys.exit(0)"
    $oldEa = $ErrorActionPreference
    $ErrorActionPreference = 'SilentlyContinue'
    & $PythonExe @($PythonPrefix + @('-c', $code)) 2>$null | Out-Null
    $ok = ($LASTEXITCODE -eq 0)
    $ErrorActionPreference = $oldEa
    return $ok
}

$py = Get-PythonLaunch
if (-not $py) {
    Write-Error '[graphsuite] python / py -3 が見つかりません。PATH を確認してください。'
    exit 1
}

Write-Host "[graphsuite] freeing listeners on :$API_PORT (api) and webui ports $WEBUI_PORT $($WEBUI_FALLBACK_PORTS -join ' ')..."
Free-TcpListenPort -Port $API_PORT
Free-TcpListenPort -Port $WEBUI_PORT
foreach ($p in $WEBUI_FALLBACK_PORTS) {
    $portNum = 0
    if ([int]::TryParse($p, [ref]$portNum)) {
        Free-TcpListenPort -Port $portNum
    }
}
Start-Sleep -Seconds 1

$env:PYTHONIOENCODING = 'utf-8'

# Governance remediation NDJSON（オプション）。
# マシン環境に GRAPHSUITE_DEBUG_REMEDIATION=0 が残るとログが出ない。維持: GRAPHSUITE_PRESERVE_DEBUG_REMEDIATION=1
$hadDbgRem = $env:GRAPHSUITE_DEBUG_REMEDIATION
if ($env:GRAPHSUITE_PRESERVE_DEBUG_REMEDIATION -ne '1') {
    Remove-Item Env:GRAPHSUITE_DEBUG_REMEDIATION -ErrorAction SilentlyContinue
    if ($hadDbgRem -and $hadDbgRem.Trim() -ne '') {
        Write-Host "[graphsuite] ローカル起動のため GRAPHSUITE_DEBUG_REMEDIATION を外し、既定で 1 にします（NDJSON: graphsuite-governance-remediation.ndjson）。無効化を維持したい場合は GRAPHSUITE_PRESERVE_DEBUG_REMEDIATION=1。"
    }
}
if (-not $env:GRAPHSUITE_DEBUG_REMEDIATION) {
    $env:GRAPHSUITE_DEBUG_REMEDIATION = '1'
}
if (-not $env:GRAPHSUITE_DEBUG_REMEDIATION_LOG_PATH) {
    $env:GRAPHSUITE_DEBUG_REMEDIATION_LOG_PATH = (Join-Path $RootDir 'graphsuite-governance-remediation.ndjson')
}
# WebUI 側の診断（ingest / console）。無効化: $env:VITE_DEBUG_GOVERNANCE_REMEDIATION='false'
if (-not $env:VITE_DEBUG_GOVERNANCE_REMEDIATION) {
    $env:VITE_DEBUG_GOVERNANCE_REMEDIATION = 'true'
}
# webui/.env* の VITE_BACKEND_URL がリモートでも、ローカル API（Vite プロキシ）を使う。リモート API を使う場合は GRAPHSUITE_PRESERVE_VITE_BACKEND_URL=1。
if ($env:GRAPHSUITE_PRESERVE_VITE_BACKEND_URL -ne '1') {
    $env:VITE_USE_VITE_PROXY = '1'
} else {
    Remove-Item Env:VITE_USE_VITE_PROXY -ErrorAction SilentlyContinue
    if ($env:VITE_BACKEND_URL -and $env:VITE_BACKEND_URL.Trim() -ne '') {
        Write-Host "[graphsuite] VITE_USE_VITE_PROXY 無効 — ブラウザは VITE_BACKEND_URL=$($env:VITE_BACKEND_URL) に直接リクエスト（GRAPHSUITE_PRESERVE_VITE_BACKEND_URL=1）。"
    }
}

# マシン/ユーザー環境に GOVERNANCE_REMEDIATION_DISABLE_STUB_409_FALLBACK=1 が残るとスタブ Lambda 409 のまま承認できない。
# このスクリプトから起動する API 子プロセスには渡さない（保持したい場合は GRAPHSUITE_PRESERVE_DISABLE_STUB_409_FALLBACK=1）。
# PRESERVE は起動時の除去のみ。STRICT は下でセッションから外す（維持: GRAPHSUITE_PRESERVE_STRICT_STUB_409_NO_LOCAL=1）。
$hadDisStub409 = $env:GOVERNANCE_REMEDIATION_DISABLE_STUB_409_FALLBACK
if ($env:GRAPHSUITE_PRESERVE_DISABLE_STUB_409_FALLBACK -ne '1') {
    Remove-Item Env:GOVERNANCE_REMEDIATION_DISABLE_STUB_409_FALLBACK -ErrorAction SilentlyContinue
    if ($hadDisStub409 -and @('1', 'true', 'yes', 'on') -contains ($hadDisStub409.Trim().ToLower())) {
        Write-Host "[graphsuite] ローカル起動のため GOVERNANCE_REMEDIATION_DISABLE_STUB_409_FALLBACK をこのセッションから外しました（API は in-repo 再試行可）。無効化を維持したい場合は GRAPHSUITE_PRESERVE_DISABLE_STUB_409_FALLBACK=1 で run.ps1 を実行。"
    }
} elseif ($hadDisStub409 -and @('1', 'true', 'yes', 'on') -contains ($hadDisStub409.Trim().ToLower())) {
    Write-Host "[graphsuite] GOVERNANCE_REMEDIATION_DISABLE_STUB_409_FALLBACK=$hadDisStub409 — in-repo 再試行は無効（GRAPHSUITE_PRESERVE_DISABLE_STUB_409_FALLBACK=1）。"
}
# pytest 等で GRAPHSUITE_BLOCK_PLACEHOLDER_409_DISABLE_CLEAR が残ると placeholder_409_clear_blocked=true のままになる。
$hadBlockPh409 = $env:GRAPHSUITE_BLOCK_PLACEHOLDER_409_DISABLE_CLEAR
if ($env:GRAPHSUITE_PRESERVE_BLOCK_PLACEHOLDER_409_DISABLE_CLEAR -ne '1') {
    Remove-Item Env:GRAPHSUITE_BLOCK_PLACEHOLDER_409_DISABLE_CLEAR -ErrorAction SilentlyContinue
    if ($hadBlockPh409 -and $hadBlockPh409.Trim() -ne '') {
        Write-Host "[graphsuite] ローカル起動のため GRAPHSUITE_BLOCK_PLACEHOLDER_409_DISABLE_CLEAR を外しました（スタブ 409 の in-repo 再試行可）。維持する場合は GRAPHSUITE_PRESERVE_BLOCK_PLACEHOLDER_409_DISABLE_CLEAR=1。"
    }
} elseif ($hadBlockPh409 -and $hadBlockPh409.Trim() -ne '') {
    Write-Host "[graphsuite] GRAPHSUITE_BLOCK_PLACEHOLDER_409_DISABLE_CLEAR=$hadBlockPh409 — スタブ 409 のランタイム clear は無効（GRAPHSUITE_PRESERVE_BLOCK_PLACEHOLDER_409_DISABLE_CLEAR=1）。"
}
$hadStrictStub409 = $env:GRAPHSUITE_STRICT_STUB_409_NO_LOCAL
if ($env:GRAPHSUITE_PRESERVE_STRICT_STUB_409_NO_LOCAL -ne '1') {
    Remove-Item Env:GRAPHSUITE_STRICT_STUB_409_NO_LOCAL -ErrorAction SilentlyContinue
    if ($hadStrictStub409 -and $hadStrictStub409.Trim() -ne '') {
        Write-Host "[graphsuite] ローカル起動のため GRAPHSUITE_STRICT_STUB_409_NO_LOCAL を外しました（スタブ 409 in-repo 再試行可）。維持する場合は GRAPHSUITE_PRESERVE_STRICT_STUB_409_NO_LOCAL=1。"
    }
} elseif ($hadStrictStub409 -and $hadStrictStub409.Trim() -ne '') {
    Write-Host "[graphsuite] GRAPHSUITE_STRICT_STUB_409_NO_LOCAL=$hadStrictStub409 — スタブ 409 の strict オプトアウト維持（GRAPHSUITE_PRESERVE_STRICT_STUB_409_NO_LOCAL=1）。"
}
$legStub409 = $env:GOVERNANCE_REMEDIATION_STUB_409_FALLBACK
if ($legStub409 -and $legStub409.Trim() -ne '') {
    Write-Host "[graphsuite] NOTE: GOVERNANCE_REMEDIATION_STUB_409_FALLBACK は無視されます。無効化は GOVERNANCE_REMEDIATION_DISABLE_STUB_409_FALLBACK=1 を使用してください。"
}

# graphsuite_server の uvicorn access ログは LOG_DIR（既定: プロセスの cwd）。別シェル cwd だと別パスの graphsuite.log になるため固定する。
if ($env:GRAPHSUITE_PRESERVE_LOG_DIR -ne '1') {
    $env:LOG_DIR = $ApiDir
}

$apiArgs = $py.Prefix + @(
    (Join-Path $ApiDir 'graphsuite_server.py'),
    '--port',
    "$API_PORT"
)

Write-Host "[graphsuite] starting api on :$API_PORT (remediation debug log: $($env:GRAPHSUITE_DEBUG_REMEDIATION_LOG_PATH))"
$accessLogDir = if ($env:LOG_DIR -and $env:LOG_DIR.Trim() -ne '') { $env:LOG_DIR } else { $ApiDir }
Write-Host "[graphsuite] uvicorn access log: $(Join-Path $accessLogDir 'graphsuite.log')"
Write-Host "[graphsuite] governance remediation NDJSON (optional): $(Join-Path $ApiDir 'graphsuite-governance-remediation.ndjson')"
$apiProcess = Start-Process -FilePath $py.Exe -ArgumentList $apiArgs -WorkingDirectory $ApiDir `
    -PassThru -NoNewWindow

$ready = $false
for ($i = 0; $i -lt 40; $i++) {
    if ($apiProcess.HasExited) {
        Write-Error "[graphsuite] API プロセスが起動直後に終了しました (exit=$($apiProcess.ExitCode))。"
        exit 1
    }
    if (Test-ApiHealth -PythonExe $py.Exe -PythonPrefix $py.Prefix -Port $API_PORT) {
        $ready = $true
        break
    }
    Start-Sleep -Milliseconds 500
}

if (-not $ready) {
    if (-not $apiProcess.HasExited) {
        Stop-Process -Id $apiProcess.Id -Force -ErrorAction SilentlyContinue
    }
    Write-Error "[graphsuite] API が :$API_PORT で応答しません。ポート占有または起動失敗を確認してください。"
    exit 1
}

try {
    $hz = Invoke-WebRequest -Uri "http://127.0.0.1:$API_PORT/healthz" -UseBasicParsing -TimeoutSec 3
    Write-Host "[graphsuite] runtime check: GET http://127.0.0.1:$API_PORT/healthz -> $($hz.StatusCode)"
}
catch {
    Write-Warning "[graphsuite] runtime check: GET /healthz failed after ready: $($_.Exception.Message)"
}

if (-not $env:VITE_PROXY_TARGET) {
    $env:VITE_PROXY_TARGET = "http://127.0.0.1:$API_PORT"
}

Write-Host "[graphsuite] starting webui on :$WEBUI_PORT"
Write-Host "[graphsuite] api pid=$($apiProcess.Id)"
Write-Host "[graphsuite] open http://localhost:$WEBUI_PORT/"
Write-Host "[graphsuite] Ctrl+C で終了します"

try {
    Push-Location $WebuiDir
    # npm run dev は npm が --port を解釈してしまうため、Vite を直接起動する
    npx vite --host --port $WEBUI_PORT
}
finally {
    Pop-Location
    if ($apiProcess -and -not $apiProcess.HasExited) {
        Stop-Process -Id $apiProcess.Id -Force -ErrorAction SilentlyContinue
    }
}
