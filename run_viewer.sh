#!/usr/bin/env bash
# ホスト用エントリ: viewer を Docker で起動（または TP_USE_DOCKER=0 で直接実行）。

set -e
TP_ROOT="${TP_TESTER_ROOT:-$(cd "$(dirname "$0")" && pwd)}"
cd "$TP_ROOT"

if command -v docker-compose &>/dev/null && [[ "${TP_USE_DOCKER:-1}" != "0" ]]; then
  exec ./compose.sh run --rm \
    -e DISPLAY="${DISPLAY:-}" \
    -v /tmp/.X11-unix:/tmp/.X11-unix \
    tp_tester ./run_viewer_docker.sh
fi

exec ./run_viewer_docker.sh
