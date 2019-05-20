#!/usr/bin/env bash
set -xe
./scripts/mark-cleanup.sh
export VERSION=latest
export DOCKER_TAG=latest
export REPOSITORY=`hostname`.local:5000/dotmesh
make create_context
make rebuild
