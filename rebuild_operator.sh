#!/usr/bin/env bash

. build_setup.sh

# operator
dazel build //cmd/operator:operator

# TODO tagging?
if [ -z "${NO_PUSH}" ]; then
    echo "pushing image"
    dazel run //cmd/operator:operator_push
fi
