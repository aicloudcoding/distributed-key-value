#!/usr/bin/env bash
# Runs the store, e.g. ./kv.sh demo   (builds first if needed).
set -euo pipefail
cd "$(dirname "$0")"
[ -d out/production ] || ./build.sh
exec java -cp out/production kv.Main "$@"
