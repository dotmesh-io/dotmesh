#!/usr/bin/env bash
if [ "$1" == "" ]; then
    echo "Must specify Linux or Darwin as first argument"
    exit 1
fi
export PATH=/usr/local/go/bin:$PATH
set -xe
mkdir -p ../../binaries/$1
CGO_ENABLED=0 GOOS=${1,,} godep go build -a -installsuffix cgo -ldflags "-X main.clientVersion=$(cd ../versioner && go run versioner.go) -s" -o ../../binaries/$1/dm .
