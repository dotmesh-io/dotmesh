#!/bin/bash

export DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

# XXX: whoever tries to make the dev.sh stuff work on Linux will find that GOOS
# needs to be lowercase but that $(uname -s) in dev.sh forces it to be
# uppercase. Good luck, brave soul!  (Maybe `uname -s |tr "[:upper:]"
# "[:lower:]"` will help you.)
export GOOS=${GOOS:="darwin"}

docker build -t dotmesh-cli-builder -f Dockerfile.build .

OUTPUT_DIR="${DIR}/../../binaries/${GOOS}"
VERSION=$(cd ../versioner && go run versioner.go)

echo "building dm for ${GOOS} into ${OUTPUT_DIR}"

docker run -ti --rm \
  -v "${DIR}:/go/src/github.com/dotmesh-io/dotmesh/cmd/dm" \
  -v "${OUTPUT_DIR}:/target" \
  -e GOOS \
  -e CGO_ENABLED=0 \
  --entrypoint godep \
  dotmesh-cli-builder \
  go build -a -installsuffix cgo -ldflags "-X main.clientVersion="${VERSION}" -s" -o /target/dm .
