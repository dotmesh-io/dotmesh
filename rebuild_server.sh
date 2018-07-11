#!/usr/bin/env bash

# NOTE - I have split these apart so there is an option to rebuild only the
# dotmesh server without building the k8s binaries
# this yields a big time saving when only developing the frontend stack
# without k8s - export DISABLE_K8S=1 disables the k8s build
set -xe

. build_setup.sh

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
    # test tooling, built but not released:

    # dind-flexvolume
    echo "creating container: dotmesh-builder-dind-flexvolume-$ARTEFACT_CONTAINER"
    docker rm -f dotmesh-builder-dind-flexvolume-$ARTEFACT_CONTAINER || true
    docker run \
        --name dotmesh-builder-dind-flexvolume-$ARTEFACT_CONTAINER \
        -v dotmesh_build_cache_dind_flexvolume:/gocache \
        -e GOPATH=/go -e GOCACHE=/gocache \
        -w /go/src/github.com/dotmesh-io/dotmesh/cmd/dotmesh-server/pkg/dind-flexvolume \
        dotmesh-builder:$ARTEFACT_CONTAINER \
        go build -pkgdir /go/pkg -o /target/dind-flexvolume
    echo "copy binary: /target/dind-flexvolume"
    docker cp dotmesh-builder-dind-flexvolume-$ARTEFACT_CONTAINER:/target/dind-flexvolume target/
    docker rm -f dotmesh-builder-dind-flexvolume-$ARTEFACT_CONTAINER

    # dind-provisioner
    echo "creating container: dotmesh-builder-dind-provisioner-$ARTEFACT_CONTAINER"
    docker rm -f dotmesh-builder-dind-provisioner-$ARTEFACT_CONTAINER || true
    docker run \
        --name dotmesh-builder-dind-provisioner-$ARTEFACT_CONTAINER \
        -v dotmesh_build_cache_dind_provisioner:/gocache \
        -e GOPATH=/go -e GOCACHE=/gocache \
        -e CGO_ENABLED=0 \
        -w /go/src/github.com/dotmesh-io/dotmesh/cmd/dotmesh-server/pkg/dind-dynamic-provisioning \
        dotmesh-builder:$ARTEFACT_CONTAINER \
        go build -pkgdir /go/pkg -ldflags '-extldflags "-static"' -o /target/dind-provisioner .
    echo "copy binary: /target/dind-provisioner"
    docker cp dotmesh-builder-dind-provisioner-$ARTEFACT_CONTAINER:/target/dind-provisioner target/
    docker rm -f dotmesh-builder-dind-provisioner-$ARTEFACT_CONTAINER

    echo "building image: ${CI_DOCKER_DIND_PROVISIONER_IMAGE}"
    docker build -f cmd/dotmesh-server/pkg/dind-dynamic-provisioning/Dockerfile -t "${CI_DOCKER_DIND_PROVISIONER_IMAGE}" .
fi

# dotmesh-server
echo "creating container: dotmesh-builder-server-$ARTEFACT_CONTAINER"
docker rm -f dotmesh-builder-server-$ARTEFACT_CONTAINER || true
docker run \
    --name dotmesh-builder-server-$ARTEFACT_CONTAINER \
    -v dotmesh_build_cache_server:/gocache \
    -e GOPATH=/go -e GOCACHE=/gocache \
    -w /go/src/github.com/dotmesh-io/dotmesh/cmd/dotmesh-server \
    dotmesh-builder:$ARTEFACT_CONTAINER \
    go build -pkgdir /go/pkg -ldflags "-X main.serverVersion=${VERSION}" -o /target/dotmesh-server
echo "copy binary: /target/dotmesh-server"
docker cp dotmesh-builder-server-$ARTEFACT_CONTAINER:/target/dotmesh-server target/
docker rm -f dotmesh-builder-server-$ARTEFACT_CONTAINER
echo "building image: ${CI_DOCKER_SERVER_IMAGE}"

docker build -f cmd/dotmesh-server/Dockerfile -t "${CI_DOCKER_SERVER_IMAGE}" .

# allow disabling of registry push
if [ -z "${NO_PUSH}" ]; then
    echo "pushing images"
    docker push ${CI_DOCKER_SERVER_IMAGE}
    if [ -z "${SKIP_K8S}" ]; then
        docker push ${CI_DOCKER_PROVISIONER_IMAGE}
        echo "pushing dind provisioner"
        docker push ${CI_DOCKER_DIND_PROVISIONER_IMAGE}
        echo "pushing operator"
        docker push ${CI_DOCKER_OPERATOR_IMAGE}
    fi
fi
