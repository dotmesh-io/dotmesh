#!/usr/bin/env bash
set -xe

. build_setup.sh

location=$(realpath .)/bazel-bin/cmd
echo "Output path inside dazel will be $location"
./rebuild_client.sh $1
if [ -z "${SKIP_K8S}" ]; then
    ./rebuild_provisioner.sh
    ./rebuild_flexvolume.sh
    ./rebuild_operator.sh
fi
./rebuild_server.sh
(cd kubernetes && ./rebuild.sh)
