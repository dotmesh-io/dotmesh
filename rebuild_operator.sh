#!/usr/bin/env bash

. build_setup.sh

# operator
dazel build //cmd/operator:operator
docker cp dazel:$location/operator/linux_amd64_stripped/operator target/

# TODO push using bazel
if [ -z "${NO_PUSH}" ]; then
    echo "pushing image"
    docker push ${CI_DOCKER_OPERATOR_IMAGE}
fi
