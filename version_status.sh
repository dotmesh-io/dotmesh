#!/bin/bash

# script to tell bazel what version to set on the client
VERSION=$(cd cmd/versioner && go run versioner.go)
echo VERSION ${VERSION}
if [ -z "$CI_DOCKER_TAG" ]; then
    # Non-CI build
    DOCKERTAG=latest
else
    DOCKERTAG=$CI_DOCKER_TAG
fi

REGISTRY=${CI_REGISTRY:-$(hostname).local:80}
echo DOCKERTAG ${DOCKERTAG}
echo CI_REGISTRY ${REGISTRY}
echo DOTMESH_SERVER_IMAGE ${REGISTRY}/dotmesh/dotmesh-server:${DOCKERTAG}