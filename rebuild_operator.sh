#!/usr/bin/env bash

. build_setup.sh

# operator
dazel build //cmd/operator:operator
docker cp dazel:$location/operator/linux_amd64_stripped/operator target/

# TODO build using bazel
echo "building image: ${CI_DOCKER_OPERATOR_IMAGE}"
docker build -f cmd/operator/Dockerfile -t "${CI_DOCKER_OPERATOR_IMAGE}" .

if [ -z "${NO_PUSH}" ]; then
    echo "pushing image"
    docker push ${CI_DOCKER_OPERATOR_IMAGE}
fi
