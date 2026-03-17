#!/usr/bin/env bash
# Docker 内用（またはホストで直接実行する用）。nrc_ws を source して評価のみを実行。

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

export TP_TESTER_ROOT="$TP_ROOT"
python3 scripts/run_evaluation.py "$@"
