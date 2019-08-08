#!/usr/bin/env bash
set -xe
./scripts/mark-cleanup.sh
export VERSION=latest
export DOCKER_TAG=latest
export CI_REGISTRY="$(hostname).local:80"
export CI_REPOSITORY="dotmesh"
export REPOSITORY="${CI_REGISTRY}/${CI_REPOSITORY}/"
make release_all
make build_client
