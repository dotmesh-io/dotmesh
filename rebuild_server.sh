#!/usr/bin/env bash

# NOTE - I have split these apart so there is an option to rebuild only the
# dotmesh server without building the k8s binaries
# this yields a big time saving when only developing the frontend stack
# without k8s - export DISABLE_K8S=1 disables the k8s build
set -xe

. build_setup.sh
location=$(realpath .)/bazel-bin/cmd
# docker
echo "creating container: dotmesh-builder-docker-$ARTEFACT_CONTAINER"
docker rm -f dotmesh-builder-docker-$ARTEFACT_CONTAINER || true
docker create \
    --name dotmesh-builder-docker-$ARTEFACT_CONTAINER \
    dotmesh-builder:$ARTEFACT_CONTAINER
echo "copy binary: /target/docker"
docker cp dotmesh-builder-docker-$ARTEFACT_CONTAINER:/target/docker target/
docker rm -f dotmesh-builder-docker-$ARTEFACT_CONTAINER

# skip rebuilding Kubernetes components if not using them
if [ -z "${SKIP_K8S}" ]; then
    # dind-provisioner (builds a container)
    echo "building dind-provisioner container"
    bazel build //cmd/dotmesh-server/pkg/dind-dynamic-provisioning:dind-dynamic-provisioning --platforms=@io_bazel_rules_go//go/toolchain:linux_amd64
fi

# dotmesh-server
echo "Building dotmesh-server container"
# TODO serverVersion?
bazel build //cmd/dotmesh-server:dotmesh-server-img --platforms=@io_bazel_rules_go//go/toolchain:linux_amd64
#     go build -pkgdir /go/pkg -ldflags "-X main.serverVersion=${VERSION}" -o /target/dotmesh-server
# allow disabling of registry push
if [ -z "${NO_PUSH}" ]; then
    echo "pushing images"
    bazel run //cmd/dotmesh-server:dotmesh-server_push  --platforms=@io_bazel_rules_go//go/toolchain:linux_amd64 --workspace_status_command=$(realpath ./version_status.sh)
    if [ -z "${SKIP_K8S}" ]; then
        echo "pushing dind provisioner"
        bazel run //cmd/dotmesh-server/pkg/dind-dynamic-provisioning:dind_push --workspace_status_command=$(realpath ./version_status.sh) --platforms=@io_bazel_rules_go//go/toolchain:linux_amd64
    fi
fi
