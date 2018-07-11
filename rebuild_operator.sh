#!/usr/bin/env bash

. build_setup.sh

# operator
echo "creating container: dotmesh-builder-operator-$ARTEFACT_CONTAINER"
docker rm -f dotmesh-builder-operator-$ARTEFACT_CONTAINER || true
docker run \
       --name dotmesh-builder-operator-$ARTEFACT_CONTAINER \
       -v dotmesh_build_cache_operator:/gocache \
       -e GOPATH=/go -e GOCACHE=/gocache \
       -e CGO_ENABLED=0 \
       -w /go/src/github.com/dotmesh-io/dotmesh/cmd/operator \
       dotmesh-builder:$ARTEFACT_CONTAINER \
       go build -pkgdir /go/pkg -ldflags "-extldflags \"-static\" -X main.DOTMESH_VERSION=${VERSION} -X main.DOTMESH_IMAGE=${CI_DOCKER_SERVER_IMAGE} " -o /target/operator .
echo "copy binary: /target/operator"
docker cp dotmesh-builder-operator-$ARTEFACT_CONTAINER:/target/operator target/
docker rm -f dotmesh-builder-operator-$ARTEFACT_CONTAINER

echo "building image: ${CI_DOCKER_OPERATOR_IMAGE}"
docker build -f cmd/operator/Dockerfile -t "${CI_DOCKER_OPERATOR_IMAGE}" .
