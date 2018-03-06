#!/usr/bin/env bash
# Run with arguments you want to pass to test.
# Example: ./test.sh -run TestTwoSingleNodeClusters
set -xe
echo "=== CONTAINERS RUNNING AT START OF TEST ==="
docker ps
echo "==========================================="
export PATH=/usr/local/go/bin:$PATH
export DISABLE_LOG_AGGREGATION=1
cd tests
(sudo -E `which go` test -v "$@" 2>&1 ) | ts
