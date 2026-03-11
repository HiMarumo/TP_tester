#!/usr/bin/env bash
# Docker 内用（またはホストで直接実行する用）。nrc_ws を source して baseline 作成。
# 通常は run_baseline.sh から呼ばれる（ホストでビルド → Docker でこのスクリプト）。

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

# 未設定ならコンテナ内のマウント先から commit を取得。リポジトリ (nrc_wm) のみ、パッケージ (nrc_wm_svcs) とは別。
TP_REPO="${TP_REPO:-$(python3 -c "import json; print(json.load(open('settings.json'))['node'].get('trajectory_predictor_repo','nrc_wm'))")}"
if [[ -z "${TP_BASELINE_COMMIT:-}" ]] && [[ -n "$NRC_WS_DEVEL" ]] && command -v git &>/dev/null; then
  NRC_WS_ROOT="$(dirname "$(dirname "$NRC_WS_DEVEL")")"
  for d in "$NRC_WS_ROOT/src/$TP_REPO" "$NRC_WS_ROOT/src/nrc/$TP_REPO" "$NRC_WS_ROOT"; do
    if [[ -d "$d" ]] && (git -C "$d" rev-parse HEAD &>/dev/null); then
      export TP_BASELINE_COMMIT="$(git -C "$d" rev-parse HEAD)"
      break
    fi
  done
fi

export TP_TESTER_ROOT="$TP_ROOT"
python3 scripts/run_baseline.py
