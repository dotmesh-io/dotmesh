#!/usr/bin/env bash
# Run with arguments you want to pass to test.
# Example: ./test.sh -run TestTwoSingleNodeClusters
set -xe
set -o pipefail
export PATH=/usr/local/go/bin:$PATH
export DISABLE_LOG_AGGREGATION=1
cd tests
(sudo -E `which go` test -v -timeout 9m "$@" 2>&1 ) | ts
