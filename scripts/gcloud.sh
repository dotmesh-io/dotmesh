#!/bin/bash -e

set -e

declare -a REQUIRED=("CI_DOCKER_REGISTRY" "QUAY_USER" "QUAY_PASSWORD" "GCLOUD_SERVICE_KEY")
for field in "${REQUIRED[@]}"
do
  eval "value=\$$field"
  if [ -z "$value" ]; then
    echo >&2 "$field needed"
    exit 1
  fi
done

GCLOUD_IMAGE="$CI_DOCKER_REGISTRY/dotmesh-gcloud:latest"

if [ "$1" == "login" ]; then
  docker login -u $QUAY_USER -p $QUAY_PASSWORD quay.io
  docker pull $GCLOUD_IMAGE
  exit 0
fi

docker run --rm $GCLOUD_DOCKER_ARGS \
  -v /var/run/docker.sock:/var/run/docker.sock \
  -v $PWD:/app $GCLOUD_DOCKER_ENV \
  -e HOSTNAME \
  -e GCLOUD_REGISTRY \
  -e GCLOUD_PROJECT_ID \
  -e GCLOUD_ZONE \
  -e GCLOUD_CLUSTER_ID \
  -e GCLOUD_SERVICE_KEY \
  -e NAMESPACE \
  -e IMAGE \
  -e VERSION \
  $GCLOUD_IMAGE "$@"
