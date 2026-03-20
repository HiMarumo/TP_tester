#!/usr/bin/env bash
# ホスト用エントリ: viewer を Docker で起動（または TP_USE_DOCKER=0 で直接実行）。

set -e
TP_ROOT="${TP_TESTER_ROOT:-$(cd "$(dirname "$0")" && pwd)}"
cd "$TP_ROOT"

# Qt warning suppression for environments where XDG_RUNTIME_DIR is unset.
if [[ -z "${XDG_RUNTIME_DIR:-}" ]]; then
  export XDG_RUNTIME_DIR="/tmp/runtime-${USER:-$(id -u)}"
fi
mkdir -p "$XDG_RUNTIME_DIR" 2>/dev/null || true
chmod 700 "$XDG_RUNTIME_DIR" 2>/dev/null || true

# Keep host rqt_image_view and container viewer on the same ROS graph.
export ROS_MASTER_URI="${ROS_MASTER_URI:-http://127.0.0.1:11311}"
export ROS_IP="${ROS_IP:-127.0.0.1}"

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
  terminate_pid "${RQT_PID:-}"
  exit "$rc"
}
trap cleanup INT TERM EXIT

if command -v rqt_image_view &>/dev/null; then
  RQT_IMAGE_TOPIC="${RQT_IMAGE_TOPIC:-/odet_cam_front_tele/image_raw}"
  RQT_IMAGE_TRANSPORT="${RQT_IMAGE_TRANSPORT:-compressed}"
  RQT_ARGS=()
  if [[ "${RQT_IMAGE_VIEW_CLEAR_CONFIG:-1}" == "1" ]]; then
    RQT_ARGS+=(--clear-config)
  fi
  RQT_ARGS+=("_image_transport:=${RQT_IMAGE_TRANSPORT}")
  if [[ -n "${RQT_IMAGE_TOPIC}" ]]; then
    RQT_ARGS+=("image:=${RQT_IMAGE_TOPIC}")
  fi
  rqt_image_view "${RQT_ARGS[@]}" &
  RQT_PID=$!
else
  echo "[viewer] warning: rqt_image_view is not installed on host; skipping launch." >&2
fi

if command -v docker-compose &>/dev/null && [[ "${TP_USE_DOCKER:-1}" != "0" ]]; then
  ./compose.sh run --rm \
    -e DISPLAY="${DISPLAY:-}" \
    -e XDG_RUNTIME_DIR="${XDG_RUNTIME_DIR:-}" \
    -e ROS_MASTER_URI="${ROS_MASTER_URI:-}" \
    -e ROS_IP="${ROS_IP:-}" \
    -e TP_SKIP_RQT_IMAGE_VIEW=1 \
    -v /tmp/.X11-unix:/tmp/.X11-unix \
    tp_tester_viewer ./run_viewer_docker.sh
  trap - INT TERM EXIT
  terminate_pid "${RQT_PID:-}"
  exit $?
fi

TP_SKIP_RQT_IMAGE_VIEW=1 ./run_viewer_docker.sh
trap - INT TERM EXIT
terminate_pid "${RQT_PID:-}"
