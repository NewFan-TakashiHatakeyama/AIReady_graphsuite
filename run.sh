#!/usr/bin/env bash

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
API_DIR="$ROOT_DIR/api"
WEBUI_DIR="$ROOT_DIR/webui"

API_PORT="${API_PORT:-9621}"
WEBUI_PORT="${WEBUI_PORT:-5173}"
# Vite が既定ポート占有時に順に試す番号 — 先に空けておき ${WEBUI_PORT} 固定で起動する
WEBUI_FALLBACK_PORTS="${WEBUI_FALLBACK_PORTS:-5174 5175}"

API_PID=""
WEBUI_PID=""
PYTHON_CMD=()

# Windows (Git Bash): 指定 TCP ポートの LISTEN プロセスを停止（二重起動・9621 取り違え防止）
free_tcp_listen_port() {
  local port="$1"
  [[ -z "${port}" ]] && return 0
  if command -v powershell.exe >/dev/null 2>&1; then
    powershell.exe -NoProfile -ExecutionPolicy Bypass -Command "
      \$port = ${port}
      try {
        \$conns = Get-NetTCPConnection -LocalPort \$port -State Listen -ErrorAction SilentlyContinue
        foreach (\$c in \$conns) {
          \$p = [int]\$c.OwningProcess
          if (\$p -gt 0) { Stop-Process -Id \$p -Force -ErrorAction SilentlyContinue }
        }
      } catch {}
    " >/dev/null 2>&1 || true
    return 0
  fi
  if command -v fuser >/dev/null 2>&1; then
    fuser -k "${port}/tcp" 2>/dev/null || true
    return 0
  fi
  if [[ "$(uname -s 2>/dev/null)" == "Darwin" ]] && command -v lsof >/dev/null 2>&1; then
    local pids
    pids="$(lsof -ti tcp:"${port}" -sTCP:LISTEN 2>/dev/null || true)"
    if [[ -n "${pids}" ]]; then
      kill ${pids} 2>/dev/null || true
    fi
  fi
}

free_graphsuite_dev_ports() {
  echo "[graphsuite] freeing listeners on :${API_PORT} (api) and webui ports ${WEBUI_PORT} ${WEBUI_FALLBACK_PORTS}..."
  free_tcp_listen_port "${API_PORT}"
  free_tcp_listen_port "${WEBUI_PORT}"
  local p
  for p in ${WEBUI_FALLBACK_PORTS}; do
    free_tcp_listen_port "${p}"
  done
  sleep 1
}

resolve_python_cmd() {
  if command -v python >/dev/null 2>&1; then
    PYTHON_CMD=(python)
    return 0
  fi
  if command -v python.exe >/dev/null 2>&1; then
    PYTHON_CMD=(python.exe)
    return 0
  fi
  if command -v python3 >/dev/null 2>&1; then
    PYTHON_CMD=(python3)
    return 0
  fi
  if command -v py.exe >/dev/null 2>&1; then
    PYTHON_CMD=(py.exe -3)
    return 0
  fi
  if command -v py >/dev/null 2>&1; then
    PYTHON_CMD=(py -3)
    return 0
  fi
  return 1
}

cleanup() {
  local exit_code=$?

  if [[ -n "${API_PID}" ]] && kill -0 "${API_PID}" 2>/dev/null; then
    kill "${API_PID}" 2>/dev/null || true
  fi
  if [[ -n "${WEBUI_PID}" ]] && kill -0 "${WEBUI_PID}" 2>/dev/null; then
    kill "${WEBUI_PID}" 2>/dev/null || true
  fi

  wait 2>/dev/null || true
  exit "${exit_code}"
}

trap cleanup INT TERM EXIT

wait_for_api_ready() {
  local max_attempts=40
  local attempt=1

  while (( attempt <= max_attempts )); do
    if [[ -n "${API_PID}" ]] && ! kill -0 "${API_PID}" 2>/dev/null; then
      return 1
    fi

    if "${PYTHON_CMD[@]}" -c "import sys, urllib.request; urllib.request.urlopen('http://127.0.0.1:${API_PORT}/health', timeout=1); sys.exit(0)" >/dev/null 2>&1; then
      return 0
    fi

    sleep 0.5
    ((attempt++))
  done

  return 1
}

if ! resolve_python_cmd; then
  echo "[graphsuite] failed to start api: python command not found" >&2
  echo "[graphsuite] please install Python or add python/python.exe to PATH" >&2
  exit 1
fi

free_graphsuite_dev_ports

echo "[graphsuite] starting api on :${API_PORT}"
(
  cd "${API_DIR}"
  export PYTHONIOENCODING=utf-8
  "${PYTHON_CMD[@]}" "graphsuite_server.py" --port "${API_PORT}"
) &
API_PID=$!

if ! wait_for_api_ready; then
  echo "[graphsuite] failed to start api on :${API_PORT}" >&2
  echo "[graphsuite] hint: another process may already use port ${API_PORT}, or API boot failed." >&2
  exit 1
fi

echo "[graphsuite] starting webui on :${WEBUI_PORT}"
(
  cd "${WEBUI_DIR}"
  export VITE_PROXY_TARGET="${VITE_PROXY_TARGET:-http://127.0.0.1:${API_PORT}}"
  # package.json の dev は既に vite --host のため --port のみ渡す（--host 重複を避ける）
  npm run dev -- --port "${WEBUI_PORT}"
) &
WEBUI_PID=$!

echo "[graphsuite] api pid=${API_PID}, webui pid=${WEBUI_PID}"
echo "[graphsuite] hint: after changing api/ (e.g. Connect), restart this script so Python reloads modules."
echo "[graphsuite] hint: for AWS, rebuild/redeploy the API image and redeploy Connect Lambdas if you changed connect/."
echo "[graphsuite] open http://localhost:${WEBUI_PORT}/"
echo "[graphsuite] press Ctrl+C to stop all"

wait "${API_PID}" "${WEBUI_PID}"
