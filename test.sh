#!/usr/bin/env bash
# Run with arguments you want to pass to test.
# Example: ./test.sh -run TestTwoSingleNodeClusters

if which timeout >/dev/null; then
    export TIMEOUT=timeout
elif which gtimeout >/dev/null; then
    export TIMEOUT=gtimeout
else
    echo "Unable to locate timeout or gtimeout commands (try brew/apt install coreutils)"
    exit 1
fi

if ! which uuidgen >/dev/null; then
    echo "Unable to locate uuidgen command"
    exit 1
fi

# Generate a unique test id, which we pass to go test and it puts it in the
# container-unique /dotmesh-test-pools dir, so we can find them later and clean
# them up in case the 'go test' itself times out.

export GO_TEST_ID=$(uuidgen |cut -f "-" -d 1)

(
    set -xe
    set -o pipefail
    export PATH=/usr/local/go/bin:$PATH
    export DISABLE_LOG_AGGREGATION=1
    cd tests
    if [ -n $DOTMESH_TEST_TIMEOUT ]; then
        ($TIMEOUT $DOTMESH_TEST_TIMEOUT sudo -E `which go` test -v "$@" 2>&1 ) | ts
    else
        (sudo -E `which go` test -v "$@" 2>&1 ) | ts
    fi
)
TEST_EXIT_CODE=$?

# Outside of the -xe/pipefail, after the timeout has fired, do belt-and-braces cleanup.

if [ $DOTMESH_TEST_CLEANUP == "always" ]; then
    cd /dotmesh-test-pools
    for SCRIPT in *${GO_TEST_ID}*/cleanup-actions.*; do set -x; . $SCRIPT; done
fi

exit $TEST_EXIT_CODE
