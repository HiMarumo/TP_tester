#!/usr/bin/env bash
# ホスト用エントリ: Docker 内で evaluation_docker.sh を実行（または TP_USE_DOCKER=0 で直接実行）。

set -e
TP_ROOT="${TP_TESTER_ROOT:-$(cd "$(dirname "$0")" && pwd)}"
cd "$TP_ROOT"

if command -v docker-compose &>/dev/null && [[ "${TP_USE_DOCKER:-1}" != "0" ]]; then
  exec ./compose.sh run --rm tp_tester ./evaluation_docker.sh "$@"
fi
exec ./evaluation_docker.sh "$@"
