#!/usr/bin/env bash

. build_setup.sh

# dm-provisioner
echo "creating container: dotmesh-builder-dm-provisioner-$ARTEFACT_CONTAINER"
docker rm -f dotmesh-builder-dm-provisioner-$ARTEFACT_CONTAINER || true
docker run \
       --name dotmesh-builder-dm-provisioner-$ARTEFACT_CONTAINER \
       -v dotmesh_build_cache_dm_provisioner:/gocache \
       -e GOPATH=/go -e GOCACHE=/gocache \
       -e CGO_ENABLED=0 \
       -w /go/src/github.com/dotmesh-io/dotmesh/cmd/dynamic-provisioner \
       dotmesh-builder:$ARTEFACT_CONTAINER \
       go build -pkgdir /go/pkg -ldflags '-extldflags "-static"' -o /target/dm-provisioner .
echo "copy binary: /target/dm-provisioner"
docker cp dotmesh-builder-dm-provisioner-$ARTEFACT_CONTAINER:/target/dm-provisioner target/
docker rm -f dotmesh-builder-dm-provisioner-$ARTEFACT_CONTAINER

echo "building image: ${CI_DOCKER_PROVISIONER_IMAGE}"
docker build -f cmd/dynamic-provisioner/Dockerfile -t "${CI_DOCKER_PROVISIONER_IMAGE}" .
