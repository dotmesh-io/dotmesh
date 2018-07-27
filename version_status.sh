#!/bin/bash

set -e

# script to tell bazel what version to set on the client
STABLE_VERSION=$(cd cmd/versioner && go run versioner.go)
echo STABLE_VERSION ${STABLE_VERSION}
echo STABLE_DOCKERTAG ${STABLE_DOCKERTAG}
echo CI_REGISTRY ${REGISTRY}
echo CI_REPOSITORY ${REPOSITORY}
echo STABLE_CI_DOCKER_SERVER_IMAGE ${STABLE_CI_DOCKER_SERVER_IMAGE}
