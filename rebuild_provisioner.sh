#!/usr/bin/env bash

. build_setup.sh

dazel build //cmd/dynamic-provisioner:dynamic-provisioner

# todo push using bazel
if [ -z "${NO_PUSH}" ]; then
    echo "pushing image"
    dazel run //cmd/dynamic-provisioner:provisioner_push
fi
