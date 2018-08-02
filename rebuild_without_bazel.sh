#!/bin/bash

## setup

set -ex

if [ x$1 == x--push ]
then
    PUSH=YES
    shift
fi

OS=Linux
if [ -n "$1" ]
then
    OS=$1
fi

if [ x$CI_DOCKER_TAG == x ]
then
    # Non-CI build
    CI_DOCKER_TAG=latest
fi

VERSION=$(cd cmd/versioner && go run versioner.go)

HOSTNAME=${HOSTNAME:=$(hostname).local}

export STABLE_CI_DOCKER_SERVER_IMAGE=${STABLE_CI_DOCKER_SERVER_IMAGE:=$HOSTNAME:80/dotmesh/dotmesh-server:latest}
export CI_DOCKER_PROVISIONER_IMAGE=${CI_DOCKER_PROVISIONER_IMAGE:=$HOSTNAME:80/dotmesh/dotmesh-dynamic-provisioner:latest}
export CI_DOCKER_DIND_PROVISIONER_IMAGE=${CI_DOCKER_DIND_PROVISIONER_IMAGE:=$HOSTNAME:80/dotmesh/dind-dynamic-provisioner:latest}
export CI_DOCKER_OPERATOR_IMAGE=${CI_DOCKER_OPERATOR_IMAGE:=$HOSTNAME:80/dotmesh/dotmesh-operator:latest}

export GOCACHE=`pwd`/.gocache

mkdir -p $GOCACHE

rm -rf target/* || true

## client

LOWERCASE_OS=$(echo "$OS" | tr '[:upper:]' '[:lower:]')
export GOOS=${GOOS:="$LOWERCASE_OS"}

OUTPUT_DIR="`pwd`/binaries/$OS"
mkdir -p $OUTPUT_DIR

CGO_ENABLED=0 go build -ldflags "-X main.clientVersion=${VERSION} -X main.dockerTag=$CI_DOCKER_TAG -s" -o $OUTPUT_DIR/dm ./cmd/dm

if [ -z "${SKIP_K8S}" ]
then

    ## flexvolume

    go build -o ./target/flexvolume ./cmd/flexvolume

    ## operator

    go build -ldflags "-linkmode external -extldflags \"-static\" -X main.DOTMESH_VERSION=${VERSION} -X main.DOTMESH_IMAGE=${STABLE_CI_DOCKER_SERVER_IMAGE} " -o ./target/operator ./cmd/operator

    echo "building image: ${CI_DOCKER_OPERATOR_IMAGE}"
    echo 'FROM scratch' > target/Dockerfile
    echo 'COPY ./operator /' >> target/Dockerfile
    echo 'CMD ["/operator"]' >> target/Dockerfile
    docker build -f target/Dockerfile -t "${CI_DOCKER_OPERATOR_IMAGE}" target

    if [ -n "${PUSH}" ]; then
        echo "pushing image"
        docker push ${CI_DOCKER_OPERATOR_IMAGE}
    fi

    ## provisioner

    go build -pkgdir /go/pkg -ldflags '-linkmode external -extldflags "-static"' -o ./target/dm-provisioner ./cmd/dynamic-provisioner

    echo "building image: ${CI_DOCKER_PROVISIONER_IMAGE}"
    echo 'FROM scratch' > target/Dockerfile
    echo 'COPY ./dm-provisioner /' >> target/Dockerfile
    echo 'CMD ["/dm-provisioner"]' >> target/Dockerfile
    docker build -f target/Dockerfile -t "${CI_DOCKER_PROVISIONER_IMAGE}" target

    if [ -n "${PUSH}" ]; then
        echo "pushing image"
        docker push ${CI_DOCKER_PROVISIONER_IMAGE}
    fi

    ## yaml

    (cd kubernetes; ./rebuild.sh)

    ## dind-flexvolume

    go build -o ./target/dind-flexvolume ./cmd/dotmesh-server/pkg/dind-flexvolume

    ## dind-provisioner

    go build -ldflags '-linkmode external -extldflags "-static"' -o ./target/dind-provisioner ./cmd/dotmesh-server/pkg/dind-dynamic-provisioning

    echo "building image: ${CI_DOCKER_DIND_PROVISIONER_IMAGE}"
    echo 'FROM scratch' > target/Dockerfile
    echo 'COPY ./dind-provisioner /' >> target/Dockerfile
    echo 'CMD ["/dind-provisioner"]' >> target/Dockerfile
    docker build -f target/Dockerfile -t "${CI_DOCKER_DIND_PROVISIONER_IMAGE}" target

    if [ -n "${PUSH}" ]; then
        echo "pushing image"
        docker push ${CI_DOCKER_DIND_PROVISIONER_IMAGE}
    fi
fi

## server

go build -ldflags "-X main.serverVersion=${VERSION}" -o ./target/dotmesh-server ./cmd/dotmesh-server

cp ./cmd/dotmesh-server/require_zfs.sh ./target

echo "building image: ${STABLE_CI_DOCKER_SERVER_IMAGE}"

cp cmd/dotmesh-server/Dockerfile target/Dockerfile
echo 'COPY ./flexvolume /usr/local/bin/' >> target/Dockerfile
echo 'COPY ./dotmesh-server /usr/local/bin/' >> target/Dockerfile

docker build -f target/Dockerfile -t "${STABLE_CI_DOCKER_SERVER_IMAGE}" target

if [ -n "${PUSH}" ]; then
    echo "pushing image"
    docker push ${STABLE_CI_DOCKER_SERVER_IMAGE}
fi
