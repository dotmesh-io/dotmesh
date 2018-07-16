#!/usr/bin/env bash

. build_setup.sh

# flexvolume
echo "creating container: dotmesh-builder-flexvolume-$ARTEFACT_CONTAINER"
docker rm -f dotmesh-builder-flexvolume-$ARTEFACT_CONTAINER || true
docker run \
       --name dotmesh-builder-flexvolume-$ARTEFACT_CONTAINER \
       -v dotmesh_build_cache_flexvolume:/gocache \
       -e GOPATH=/go -e GOCACHE=/gocache \
       -w /go/src/github.com/dotmesh-io/dotmesh/cmd/flexvolume \
       dotmesh-builder:$ARTEFACT_CONTAINER \
       go build -pkgdir /go/pkg -o /target/flexvolume
echo "copy binary: /target/flexvolume"
docker cp dotmesh-builder-flexvolume-$ARTEFACT_CONTAINER:/target/flexvolume target/
docker rm -f dotmesh-builder-flexvolume-$ARTEFACT_CONTAINER
