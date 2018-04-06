#!/usr/bin/env bash

# NOTE - I have split these apart so there is an option to rebuild only the
# dotmesh server without building the k8s binaries
# this yields a big time saving when only developing the frontend stack
# without k8s - export DISABLE_K8S=1 disables the k8s build
set -xe

VERSION="$(cd ../versioner && go run versioner.go)"

CI_DOCKER_SERVER_IMAGE=${CI_DOCKER_SERVER_IMAGE:=$(hostname).local:80/dotmesh/dotmesh-server:latest}
CI_DOCKER_PROVISIONER_IMAGE=${CI_DOCKER_PROVISIONER_IMAGE:=$(hostname).local:80/dotmesh/dotmesh-dynamic-provisioner:latest}
CI_DOCKER_DIND_PROVISIONER_IMAGE=${CI_DOCKER_DIND_PROVISIONER_IMAGE:=$(hostname).local:80/dotmesh/dind-dynamic-provisioner:latest}

if [ -z "$CI_DOCKER_TAG" ]; then
    # Non-CI build
    ARTEFACT_CONTAINER=$VERSION
else
    ARTEFACT_CONTAINER="${CI_DOCKER_TAG}_${CI_JOB_ID}"
fi

mkdir -p target

echo "building image: dotmesh-builder:$ARTEFACT_CONTAINER"
docker build --build-arg VERSION="${VERSION}" -f Dockerfile.build -t dotmesh-builder:$ARTEFACT_CONTAINER .

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
    # flexvolume
    echo "creating container: dotmesh-builder-flexvolume-$ARTEFACT_CONTAINER"
    docker rm -f dotmesh-builder-flexvolume-$ARTEFACT_CONTAINER || true
    docker run \
        --name dotmesh-builder-flexvolume-$ARTEFACT_CONTAINER \
        -e GOPATH=/go \
        -w /go/src/github.com/dotmesh-io/dotmesh/cmd/dotmesh-server/pkg/flexvolume \
        dotmesh-builder:$ARTEFACT_CONTAINER \
        go build -o /target/flexvolume
    echo "copy binary: /target/flexvolume"
    docker cp dotmesh-builder-flexvolume-$ARTEFACT_CONTAINER:/target/flexvolume target/
    docker rm -f dotmesh-builder-flexvolume-$ARTEFACT_CONTAINER

    # dm-provisioner
    echo "creating container: dotmesh-builder-dm-provisioner-$ARTEFACT_CONTAINER"
    docker rm -f dotmesh-builder-dm-provisioner-$ARTEFACT_CONTAINER || true
    docker run \
        --name dotmesh-builder-dm-provisioner-$ARTEFACT_CONTAINER \
        -e GOPATH=/go \
        -e CGO_ENABLED=0 \
        -w /go/src/github.com/dotmesh-io/dotmesh/cmd/dotmesh-server/pkg/dynamic-provisioning \
        dotmesh-builder:$ARTEFACT_CONTAINER \
        go build -a -ldflags '-extldflags "-static"' -o /target/dm-provisioner .
    echo "copy binary: /target/dm-provisioner"
    docker cp dotmesh-builder-dm-provisioner-$ARTEFACT_CONTAINER:/target/dm-provisioner target/
    docker rm -f dotmesh-builder-dm-provisioner-$ARTEFACT_CONTAINER

    echo "building image: ${CI_DOCKER_PROVISIONER_IMAGE}"
    docker build -f pkg/dynamic-provisioning/Dockerfile -t "${CI_DOCKER_PROVISIONER_IMAGE}" .

    # test tooling, built but not released:

    # dind-flexvolume
    echo "creating container: dotmesh-builder-dind-flexvolume-$ARTEFACT_CONTAINER"
    docker rm -f dotmesh-builder-dind-flexvolume-$ARTEFACT_CONTAINER || true
    docker run \
        --name dotmesh-builder-dind-flexvolume-$ARTEFACT_CONTAINER \
        -e GOPATH=/go \
        -w /go/src/github.com/dotmesh-io/dotmesh/cmd/dotmesh-server/pkg/dind-flexvolume \
        dotmesh-builder:$ARTEFACT_CONTAINER \
        go build -o /target/dind-flexvolume
    echo "copy binary: /target/dind-flexvolume"
    docker cp dotmesh-builder-dind-flexvolume-$ARTEFACT_CONTAINER:/target/dind-flexvolume target/
    docker rm -f dotmesh-builder-dind-flexvolume-$ARTEFACT_CONTAINER

    # dind-provisioner
    echo "creating container: dotmesh-builder-dind-provisioner-$ARTEFACT_CONTAINER"
    docker rm -f dotmesh-builder-dind-provisioner-$ARTEFACT_CONTAINER || true
    docker run \
        --name dotmesh-builder-dind-provisioner-$ARTEFACT_CONTAINER \
        -e GOPATH=/go \
        -e CGO_ENABLED=0 \
        -w /go/src/github.com/dotmesh-io/dotmesh/cmd/dotmesh-server/pkg/dind-dynamic-provisioning \
        dotmesh-builder:$ARTEFACT_CONTAINER \
        go build -a -ldflags '-extldflags "-static"' -o /target/dind-provisioner .
    echo "copy binary: /target/dind-provisioner"
    docker cp dotmesh-builder-dind-provisioner-$ARTEFACT_CONTAINER:/target/dind-provisioner target/
    docker rm -f dotmesh-builder-dind-provisioner-$ARTEFACT_CONTAINER

    echo "building image: ${CI_DOCKER_DIND_PROVISIONER_IMAGE}"
    docker build -f pkg/dind-dynamic-provisioning/Dockerfile -t "${CI_DOCKER_DIND_PROVISIONER_IMAGE}" .
fi

# dotmesh-server
echo "creating container: dotmesh-builder-server-$ARTEFACT_CONTAINER"
docker rm -f dotmesh-builder-server-$ARTEFACT_CONTAINER || true
docker run \
    --name dotmesh-builder-server-$ARTEFACT_CONTAINER \
    -e GOPATH=/go \
    -w /go/src/github.com/dotmesh-io/dotmesh/cmd/dotmesh-server/pkg/main \
    dotmesh-builder:$ARTEFACT_CONTAINER \
    go build -ldflags "-X main.serverVersion=${VERSION}" -o /target/dotmesh-server
echo "copy binary: /target/dotmesh-server"
docker cp dotmesh-builder-server-$ARTEFACT_CONTAINER:/target/dotmesh-server target/
docker rm -f dotmesh-builder-server-$ARTEFACT_CONTAINER
echo "building image: ${CI_DOCKER_SERVER_IMAGE}"

docker build -t "${CI_DOCKER_SERVER_IMAGE}" .

# allow disabling of registry push
if [ -z "${NO_PUSH}" ]; then
    docker push ${CI_DOCKER_SERVER_IMAGE}
    docker push ${CI_DOCKER_PROVISIONER_IMAGE}
fi
