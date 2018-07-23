#!/usr/bin/env bash
set -xe

source lib.sh

main() {
    export CI_REGISTRY=$1
    bazel-with-workspace run //cmd/dotmesh-server:dotmesh-server_push
    bazel-with-workspace run //cmd/operator:operator_push
    bazel-with-workspace run //cmd/dynamic-provisioner:provisioner_push
}

main