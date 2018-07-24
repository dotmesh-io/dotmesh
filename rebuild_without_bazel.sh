#!/bin/bash

## setup

set -ex

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

export CI_DOCKER_SERVER_IMAGE=${CI_DOCKER_SERVER_IMAGE:=$(hostname).local:80/dotmesh/dotmesh-server:latest}
export CI_DOCKER_PROVISIONER_IMAGE=${CI_DOCKER_PROVISIONER_IMAGE:=$(hostname).local:80/dotmesh/dotmesh-dynamic-provisioner:latest}
export CI_DOCKER_DIND_PROVISIONER_IMAGE=${CI_DOCKER_DIND_PROVISIONER_IMAGE:=$(hostname).local:80/dotmesh/dind-dynamic-provisioner:latest}
export CI_DOCKER_OPERATOR_IMAGE=${CI_DOCKER_OPERATOR_IMAGE:=$(hostname).local:80/dotmesh/dotmesh-operator:latest}

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

    go build -ldflags "-extldflags \"-static\" -X main.DOTMESH_VERSION=${VERSION} -X main.DOTMESH_IMAGE=${CI_DOCKER_SERVER_IMAGE} " -o ./target/operator ./cmd/operator

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

    go build -pkgdir /go/pkg -ldflags '-extldflags "-static"' -o ./target/dm-provisioner ./cmd/dynamic-provisioner

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

    CGO_ENABLED=0 go build -ldflags '-extldflags "-static"' -o ./target/dind-provisioner ./cmd/dotmesh-server/pkg/dind-dynamic-provisioning

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

echo "building image: ${CI_DOCKER_SERVER_IMAGE}"

echo 'FROM ubuntu:artful' > target/Dockerfile
echo 'ENV SECURITY_UPDATES 2018-01-19' >> target/Dockerfile
echo 'RUN apt-get -y update && apt-get -y install zfsutils-linux iproute kmod curl' >> target/Dockerfile
# Merge kernel module search paths from CentOS and Ubuntu :-O
echo "RUN echo 'search updates extra ubuntu built-in weak-updates' > /etc/depmod.d/ubuntu.conf" >> target/Dockerfile
echo 'ADD ./require_zfs.sh /require_zfs.sh' >> target/Dockerfile
echo 'COPY ./flexvolume /usr/local/bin/' >> target/Dockerfile
echo 'COPY ./dotmesh-server /usr/local/bin/' >> target/Dockerfile

docker build -f target/Dockerfile -t "${CI_DOCKER_SERVER_IMAGE}" target

if [ -n "${PUSH}" ]; then
    echo "pushing image"
    docker push ${CI_DOCKER_SERVER_IMAGE}
fi
