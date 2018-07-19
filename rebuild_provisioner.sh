#!/usr/bin/env bash

. build_setup.sh

dazel build //cmd/dynamic-provisioner:dynamic-provisioner
docker cp dazel:$location/dynamic-provisioner/linux_amd64_stripped/dynamic-provisioner target/

# todo push using bazel
if [ -z "${NO_PUSH}" ]; then
    echo "pushing image"
    docker push ${CI_DOCKER_PROVISIONER_IMAGE}
fi
