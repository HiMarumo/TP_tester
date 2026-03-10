#!/usr/bin/env bash
# ホスト用エントリ: nrc_ws をビルドしてから Docker で run_test_docker.sh を実行（または TP_USE_DOCKER=0 で直接実行）。

set -e
TP_ROOT="${TP_TESTER_ROOT:-$(cd "$(dirname "$0")" && pwd)}"
cd "$TP_ROOT"

NRC_WS_DEVEL="${NRC_WS_DEVEL:-$(python3 -c "import json; print(json.load(open('settings.json'))['paths'].get('nrc_ws_devel','/home/nissan/projects/nrc_ws/devel/setup.bash'))")}"
NRC_WS_ROOT="$(dirname "$(dirname "$NRC_WS_DEVEL")")"
if [[ "$NRC_WS_DEVEL" != /* ]]; then
  NRC_WS_DEVEL="$TP_ROOT/$NRC_WS_DEVEL"
  NRC_WS_ROOT="$(cd "$(dirname "$(dirname "$NRC_WS_DEVEL")")" && pwd)"
fi
if [[ -f "$NRC_WS_DEVEL" ]] && [[ -d "$NRC_WS_ROOT" ]]; then
  echo "[run_test] ホストで nrc_wm_svcs をビルドします: $NRC_WS_ROOT"
  (source "$NRC_WS_DEVEL" && cd "$NRC_WS_ROOT" && catkin_make --pkg nrc_wm_svcs) || exit 1
fi

# コミット取得は git リポジトリ (nrc_wm) のみ。パッケージ (nrc_wm_svcs) とは別。
TP_REPO="${TP_REPO:-$(python3 -c "import json; print(json.load(open('settings.json'))['node'].get('trajectory_predictor_repo','nrc_wm'))")}"
if [[ -z "${TP_TEST_COMMIT:-}" ]] && [[ -d "$NRC_WS_ROOT" ]] && command -v git &>/dev/null; then
  for d in "$NRC_WS_ROOT/src/$TP_REPO" "$NRC_WS_ROOT/src/nrc/$TP_REPO" "$NRC_WS_ROOT"; do
    if [[ -d "$d" ]] && (git -C "$d" rev-parse HEAD &>/dev/null); then
      export TP_TEST_COMMIT="$(git -C "$d" rev-parse HEAD)"
      echo "[run_test] ホストで commit を取得しました: $TP_TEST_COMMIT"
      break
    fi
  done
fi
if [[ -z "${TP_TEST_COMMIT:-}" ]] && [[ -d "$NRC_WS_ROOT" ]]; then
  echo "[run_test] ホストで git リポジトリが見つかりませんでした (NRC_WS_ROOT=$NRC_WS_ROOT)。手動で TP_TEST_COMMIT=<hash> を指定してください。" >&2
fi

if command -v docker-compose &>/dev/null && [[ "${TP_USE_DOCKER:-1}" != "0" ]]; then
  exec ./compose.sh run --rm tp_tester ./run_test_docker.sh
fi
exec ./run_test_docker.sh
