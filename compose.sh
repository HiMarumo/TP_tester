#!/usr/bin/env bash
# docker-compose のラッパー。apt の docker-compose で Python/urllib3 不整合が出る場合は
# PYTHONNOUSERSITE=1 で実行する（bashrc に書かずに済む）。
# 例: ./compose.sh run --rm tp_tester ./run_baseline.sh
set -e
cd "$(dirname "$0")"
# コンテナをホストの UID/GID で動かし、作成ファイルの所有者をホストユーザにする（鍵マークを防ぐ）
export UID GID
PYTHONNOUSERSITE=1 docker-compose "$@"
