#!/usr/bin/env bash
# Compiles the store into out/production.
set -euo pipefail
cd "$(dirname "$0")"
rm -rf out/production
javac --release 21 -d out/production $(find src -name '*.java')
