#!/usr/bin/env bash
# Docker 内用（またはホストで直接実行する用）。nrc_ws を source して viewer を起動。

set -e
TP_ROOT="${TP_TESTER_ROOT:-$(cd "$(dirname "$0")" && pwd)}"
cd "$TP_ROOT"

# Qt warning suppression for environments where XDG_RUNTIME_DIR is unset.
if [[ -z "${XDG_RUNTIME_DIR:-}" ]]; then
  export XDG_RUNTIME_DIR="/tmp/runtime-${USER:-$(id -u)}"
fi
mkdir -p "$XDG_RUNTIME_DIR" 2>/dev/null || true
chmod 700 "$XDG_RUNTIME_DIR" 2>/dev/null || true

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
RQT_PID=""
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
  terminate_pid "${RQT_PID:-}"
  terminate_pid "${ROSTERM_PID:-}"
  exit "$rc"
}
trap cleanup INT TERM EXIT

export TP_TESTER_ROOT="$TP_ROOT"
if [[ "${TP_SKIP_RQT_IMAGE_VIEW:-0}" != "1" ]]; then
  if command -v rqt_image_view &>/dev/null; then
    rqt_image_view &
    RQT_PID=$!
  else
    echo "[viewer] warning: rqt_image_view is not installed; skipping launch." >&2
  fi
fi
python3 scripts/viewer_app.py &
APP_PID=$!
wait "$APP_PID"
APP_PID=""
terminate_pid "${RQT_PID:-}"
RQT_PID=""

trap - INT TERM EXIT
if [[ -n "${ROSTERM_PID:-}" ]] && kill -0 "$ROSTERM_PID" 2>/dev/null; then
  kill "$ROSTERM_PID" 2>/dev/null || true
  wait "$ROSTERM_PID" 2>/dev/null || true
fi
