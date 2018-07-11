#!/usr/bin/env bash
set -xe

. build_setup.sh

(cd cmd/dm && ./rebuild.sh Linux)
if [ -z "${SKIP_K8S}" ]; then
    ./rebuild_flexvolume.sh
    ./rebuild_operator.sh
fi
./rebuild_server.sh
(cd kubernetes && ./rebuild.sh)
