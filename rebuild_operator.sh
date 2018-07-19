#!/usr/bin/env bash

. build_setup.sh

# operator
dazel build //cmd/operator:operator

if [ -z "${NO_PUSH}" ]; then
    echo "pushing image"
    dazel run //cmd/operator:operator_push --workspace_status_command ./version_status.sh
fi
