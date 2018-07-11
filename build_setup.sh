#!/usr/bin/env bash
set -xe

if [ -z "$ARTEFACT_CONTAINER" ]
then
    export VERSION="$(cd cmd/versioner && go run versioner.go)"

    if [ -z "$CI_DOCKER_TAG" ]; then
        # Non-CI build
        export ARTEFACT_CONTAINER=$VERSION
    else
        export ARTEFACT_CONTAINER="${CI_DOCKER_TAG}_${CI_JOB_ID}"
    fi

    mkdir -p target

    echo "building image: dotmesh-builder:$ARTEFACT_CONTAINER"
    docker build --build-arg VERSION="${VERSION}" -f Dockerfile.build -t dotmesh-builder:$ARTEFACT_CONTAINER .

    export CI_DOCKER_SERVER_IMAGE=${CI_DOCKER_SERVER_IMAGE:=$(hostname).local:80/dotmesh/dotmesh-server:latest}
    export CI_DOCKER_PROVISIONER_IMAGE=${CI_DOCKER_PROVISIONER_IMAGE:=$(hostname).local:80/dotmesh/dotmesh-dynamic-provisioner:latest}
    export CI_DOCKER_DIND_PROVISIONER_IMAGE=${CI_DOCKER_DIND_PROVISIONER_IMAGE:=$(hostname).local:80/dotmesh/dind-dynamic-provisioner:latest}
    export CI_DOCKER_OPERATOR_IMAGE=${CI_DOCKER_OPERATOR_IMAGE:=$(hostname).local:80/dotmesh/dotmesh-operator:latest}
fi
