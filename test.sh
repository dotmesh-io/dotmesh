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

export GO_TEST_ID=$(uuidgen |cut -d "-" -f 1)

cleanup() {
    local REASON="$1"
    echo "Cleaning up due to $REASON"

    # Outside of the -xe/pipefail, after the timeout has fired, do belt-and-braces cleanup.
    if [ "$DOTMESH_TEST_CLEANUP" == "always" ]; then
        # https://stackoverflow.com/questions/29438045/how-to-match-nothing-if-a-file-name-glob-has-no-matches
        shopt -s nullglob
        for SCRIPT in /dotmesh-test-pools/*${GO_TEST_ID}*/cleanup-actions.*; do set -x; sudo bash $SCRIPT; done
    fi
    echo "Finished cleaning up"
}

shutdown() {
    local SIGNAL="$1"
    # Remove the handler now it's happened once
    trap - $SIGNAL
    cleanup "signal $SIGNAL"
}

trap 'shutdown SIGTERM' SIGTERM
trap 'shutdown SIGINT' SIGINT
trap 'shutdown SIGQUIT' SIGQUIT
trap 'shutdown SIGHUP' SIGHUP
trap 'shutdown SIGPIPE' SIGPIPE

(
    set -xe
    set -o pipefail
    export PATH=/usr/local/go/bin:$PATH
    export DISABLE_LOG_AGGREGATION=1
    cd tests
    if [ -n $DOTMESH_TEST_TIMEOUT ]; then
        echo "======================================================"
        echo "Running with $DOTMESH_TEST_TIMEOUT timeout: go test $@"
        echo "======================================================"
        ($TIMEOUT $DOTMESH_TEST_TIMEOUT sudo -E `which go` test -v "$@" 2>&1 ) | ts
    else
        echo "======================================================"
        echo "Running without timeout: go test $@"
        echo "======================================================"
        (sudo -E `which go` test -v "$@" 2>&1 ) | ts
    fi
)
TEST_EXIT_CODE=$?
cleanup "test terminated with retval=$RETVAL"
exit $TEST_EXIT_CODE
