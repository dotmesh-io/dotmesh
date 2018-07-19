#!/usr/bin/env bash

. build_setup.sh

# flexvolume
location=$(realpath .)/bazel-bin/cmd
dazel build //cmd/flexvolume:flexvolume
docker cp dazel:$location/flexvolume/linux_amd64_stripped/flexvolume target/
