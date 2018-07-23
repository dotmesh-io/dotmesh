#!/bin/bash

# script to tell bazel what version to set on the client
VERSION=$(cd cmd/versioner && go run versioner.go)
echo VERSION ${VERSION}
if [ -z "$CI_DOCKER_TAG" ]; then
    # Non-CI build
    echo DOCKERTAG $VERSION
else
    echo DOCKERTAG $CI_DOCKER_TAG
fi

echo CI_REGISTRY ${CI_DOCKER_REGISTRY:-$(hostname).local:80}