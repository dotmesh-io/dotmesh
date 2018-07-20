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
    bazel build //cmd/dotmesh-server/pkg/dind-dynamic-provisioning:dind-dynamic-provisioning
fi

# dotmesh-server
echo "creating container: dotmesh-builder-server-$ARTEFACT_CONTAINER"
docker rm -f dotmesh-builder-server-$ARTEFACT_CONTAINER || true
# TODO serverVersion?
bazel build //cmd/dotmesh-server:dotmesh-server
# docker run \
#     --name dotmesh-builder-server-$ARTEFACT_CONTAINER \
#     -v dotmesh_build_cache_server:/gocache \
#     -e GOPATH=/go -e GOCACHE=/gocache \
#     -w /go/src/github.com/dotmesh-io/dotmesh/cmd/dotmesh-server \
#     dotmesh-builder:$ARTEFACT_CONTAINER \
#     go build -pkgdir /go/pkg -ldflags "-X main.serverVersion=${VERSION}" -o /target/dotmesh-server
echo "copy binary: /target/dotmesh-server"
docker cp dotmesh-builder-server-$ARTEFACT_CONTAINER:/target/dotmesh-server target/
docker rm -f dotmesh-builder-server-$ARTEFACT_CONTAINER
echo "building image: ${CI_DOCKER_SERVER_IMAGE}"

docker build -f cmd/dotmesh-server/Dockerfile -t "${CI_DOCKER_SERVER_IMAGE}" .

# allow disabling of registry push
if [ -z "${NO_PUSH}" ]; then
    echo "pushing images"
    bazel run //cmd/dotmesh-server:dotmesh-server_push 
    if [ -z "${SKIP_K8S}" ]; then
        echo "pushing dind provisioner"
        bazel run //cmd/dotmesh-server/pkg/dind-dynamic-provisioning:dind_push
    fi
fi
