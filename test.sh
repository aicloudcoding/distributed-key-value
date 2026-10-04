#!/usr/bin/env bash
# Builds the store and runs the cluster test suite.
set -euo pipefail
cd "$(dirname "$0")"
./build.sh
rm -rf out/test
javac --release 21 -cp out/production -d out/test $(find test -name '*.java')
java -cp out/production:out/test kv.ClusterTest 2>/dev/null
