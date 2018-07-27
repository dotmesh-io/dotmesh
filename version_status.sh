#!/bin/bash

# script to tell bazel what version to set on the client
VERSION=$(cd cmd/versioner && go run versioner.go)
echo VERSION ${VERSION}
echo DOCKERTAG ${DOCKERTAG}
echo CI_REGISTRY ${REGISTRY}
echo CI_REPOSITORY ${REPOSITORY}
echo CI_DOCKER_SERVER_IMAGE ${CI_DOCKER_SERVER_IMAGE}
