#!/usr/bin/env bash
set -xe

# Smoke test to see whether basics still work on e.g. macOS

DM=$1
VOL="volume_`date +%s`"

sudo $DM cluster reset || (sleep 30; sudo $DM cluster reset) || true

$DM cluster init --offline --image dotmesh-server

docker run --rm -i --name smoke -v $VOL:/foo --volume-driver dm ubuntu touch /foo/X
OUT=`$DM list`

if [[ $OUT == *"$VOL"* ]]; then
    echo "String '$VOL' found, yay!"
    exit 0
else
    echo "String '$VOL' not found, boo :("
    exit 1
fi
