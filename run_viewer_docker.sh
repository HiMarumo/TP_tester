#!/usr/bin/env bash
# Docker 内用（またはホストで直接実行する用）。nrc_ws を source して viewer を起動。

set -e
TP_ROOT="${TP_TESTER_ROOT:-$(cd "$(dirname "$0")" && pwd)}"
cd "$TP_ROOT"

NRC_WS_DEVEL="${NRC_WS_DEVEL:-}"
if [[ -z "$NRC_WS_DEVEL" ]]; then
  NRC_WS_DEVEL="$(python3 -c "import json; print(json.load(open('settings.json'))['paths'].get('nrc_ws_devel','/home/nissan/projects/nrc_ws/devel/setup.bash'))")"
fi
if [[ -f "$NRC_WS_DEVEL" ]]; then
  source "$NRC_WS_DEVEL"
fi

# Start roscore in background if not already running (needed for Launch viewer + Play)
ROSTERM_PID=""
if ! rostopic list &>/dev/null; then
  roscore &
  ROSTERM_PID=$!
  sleep 2
fi

APP_PID=""
terminate_pid() {
  local pid="$1"
  local i
  if [[ -z "$pid" ]] || ! kill -0 "$pid" 2>/dev/null; then
    return 0
  fi
  kill "$pid" 2>/dev/null || true
  for i in $(seq 1 20); do
    if ! kill -0 "$pid" 2>/dev/null; then
      return 0
    fi
    sleep 0.1
  done
  kill -9 "$pid" 2>/dev/null || true
}
cleanup() {
  local rc=$?
  terminate_pid "${APP_PID:-}"
  terminate_pid "${ROSTERM_PID:-}"
  exit "$rc"
}
trap cleanup INT TERM EXIT

export TP_TESTER_ROOT="$TP_ROOT"
python3 scripts/viewer_app.py &
APP_PID=$!
wait "$APP_PID"
APP_PID=""

trap - INT TERM EXIT
if [[ -n "${ROSTERM_PID:-}" ]] && kill -0 "$ROSTERM_PID" 2>/dev/null; then
  kill "$ROSTERM_PID" 2>/dev/null || true
  wait "$ROSTERM_PID" 2>/dev/null || true
fi
