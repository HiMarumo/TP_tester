#!/usr/bin/env bash
# Docker イメージをビルドする。apt の docker-compose で Python/urllib3 不整合が出る場合は
# PYTHONNOUSERSITE=1 で実行する（bashrc に書かずに済む）。
set -e
cd "$(dirname "$0")"
PYTHONNOUSERSITE=1 docker-compose build "$@"
