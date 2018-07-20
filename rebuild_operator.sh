#!/usr/bin/env bash

. build_setup.sh

# operator (builds container)
bazel build //cmd/operator:operator

if [ -z "${NO_PUSH}" ]; then
    echo "pushing image"
    bazel run //cmd/operator:operator_push --workspace_status_command=$(realpath ./version_status.sh)
fi
