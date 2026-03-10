#!/usr/bin/env bash
# Launch verification result viewer GUI (run inside Docker or after sourcing nrc_ws for rosrun/rosbag).

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

export TP_TESTER_ROOT="$TP_ROOT"
python3 scripts/viewer_app.py

# Stop roscore when viewer GUI is closed (only if we started it)
if [[ -n "${ROSTERM_PID:-}" ]]; then
  kill $ROSTERM_PID 2>/dev/null || true
fi
