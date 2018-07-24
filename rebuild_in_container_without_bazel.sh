#!/bin/bash

set -ex

WORKDIR=/tmp/rebuild.$$

mkdir -p $WORKDIR

cat > $WORKDIR/Dockerfile.build <<EOF
FROM ubuntu:bionic
ENV SECURITY_UPDATES 2018-02-24
RUN apt-get -y update && apt-get -y install curl software-properties-common
RUN add-apt-repository ppa:hnakamur/golang-1.10
RUN apt-get -y update
RUN apt-get -y install golang-go
RUN apt-get -y install git
RUN apt-get install -y docker.io
EOF

# Tag the image with the hash of the spec, so it will be intelligently
# cached without causing races when multiple versions are being used
# in parallel.

IMAGE_HASH=`sha1sum < $WORKDIR/Dockerfile.build | cut -f 1 -d ' '`

BUILDER_IMAGE="dotmesh-builder:$IMAGE_HASH"

docker build -f $WORKDIR/Dockerfile.build -t $BUILDER_IMAGE $WORKDIR

rm -rf $WORKDIR

# Docker builds leave stuff owned by root
trap "sudo chown -R `id -u` ." EXIT

docker run \
       -v $HOME/.docker:/root/.docker \
       -v /var/run:/var/run \
       -v `pwd`:/root/go/src/github.com/dotmesh-io/dotmesh \
       -v dotmesh-go-cache:/root/go/src/github.com/dotmesh-io/dotmesh/.gocache \
       -w /root/go/src/github.com/dotmesh-io/dotmesh \
       -e "CI_DOCKER_TAG=$CI_DOCKER_TAG" \
       -e "CI_DOCKER_SERVER_IMAGE=$CI_DOCKER_SERVER_IMAGE" \
       -e "CI_DOCKER_PROVISIONER_IMAGE=$CI_DOCKER_PROVISIONER_IMAGE" \
       -e "CI_DOCKER_DIND_PROVISIONER_IMAGE=$CI_DOCKER_DIND_PROVISIONER_IMAGE" \
       -e "CI_DOCKER_OPERATOR_IMAGE=$CI_DOCKER_OPERATOR_IMAGE" \
       $BUILDER_IMAGE \
       ./rebuild_without_bazel.sh "$@"
