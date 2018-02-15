#!/usr/bin/env bash

set -x

export DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

if [ "$1" == "" ]; then
    echo "Must specify Linux or Darwin as first argument"
    exit 1
fi


VERSION=$(cd ../versioner && go run versioner.go)

export GOOS=${1,,}
export PATH=/usr/local/go/bin:$PATH
mkdir -p ../../binaries/$1

if [ x$CI_DOCKER_TAG == x ]
then
	 # Non-CI build
	 CI_DOCKER_TAG=$VERSION
fi

OUTPUT_DIR="${DIR}/../../binaries/$1"
ARTEFACT_CONTAINER="${CI_DOCKER_TAG}_${GOOS}"

docker rm -f $ARTEFACT_CONTAINER || true
docker build -t dotmesh-cli-builder:$CI_DOCKER_TAG -f Dockerfile.build .

docker run -i \
  --name "${ARTEFACT_CONTAINER}" \
  -v "${DIR}:/go/src/github.com/dotmesh-io/dotmesh/cmd/dm" \
  -v "${OUTPUT_DIR}:/target" \
  -e GOOS \
  -e CGO_ENABLED=0 \
  --entrypoint go \
  dotmesh-cli-builder:$CI_DOCKER_TAG \
  build -a -installsuffix cgo -ldflags "-X main.clientVersion="${VERSION}" -X main.dockerTag=$CI_DOCKER_TAG -s" -o /target/dm .

docker cp $ARTEFACT_CONTAINER:/target/dm $OUTPUT_DIR/dm

docker rm -f $ARTEFACT_CONTAINER
