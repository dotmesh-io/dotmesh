#!/usr/bin/env bash
set -xe
./scripts/mark-cleanup.sh
export VERSION=latest
export DOCKER_TAG=latest
make create_context
make rebuild
