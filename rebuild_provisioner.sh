#!/usr/bin/env bash

. build_setup.sh

bazel build //cmd/dynamic-provisioner:dynamic-provisioner

if [ -z "${NO_PUSH}" ]; then
    echo "pushing image"
    bazel run //cmd/dynamic-provisioner:provisioner_push --workspace_status_command ./version_status.sh
fi
