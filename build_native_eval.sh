#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")" && pwd)"
SRC="$ROOT_DIR/native/tp_tester_native_eval.cpp"
OUT="$ROOT_DIR/native/libtp_tester_native_eval.so"

mkdir -p "$ROOT_DIR/native"
g++ -O3 -std=c++17 -shared -fPIC "$SRC" -o "$OUT"
echo "[native] built: $OUT"
