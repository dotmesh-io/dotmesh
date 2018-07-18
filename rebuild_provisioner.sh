#!/usr/bin/env bash

. build_setup.sh

dazel build //cmd/dynamic-provisioner:dynamic-provisioner
docker cp dazel:$location/dynamic-provisioner/linux_amd64_stripped/dynamic-provisioner target/

# TODO build with bazel?
echo "building image: ${CI_DOCKER_PROVISIONER_IMAGE}"
docker build -f cmd/dynamic-provisioner/Dockerfile -t "${CI_DOCKER_PROVISIONER_IMAGE}" .

if [ -z "${NO_PUSH}" ]; then
    echo "pushing image"
    docker push ${CI_DOCKER_PROVISIONER_IMAGE}
fi
