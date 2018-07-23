#!/usr/bin/env bash
set -xe

source lib.sh

main() {
    export CI_REGISTRY=$1
    bazel-with-workspace run //cmd/dotmesh-server:dotmesh-server_push
    # do a full rebuild on operator because it needs to know the server image link
    build-operator
    bazel-with-workspace run //cmd/dynamic-provisioner:provisioner_push
}

main