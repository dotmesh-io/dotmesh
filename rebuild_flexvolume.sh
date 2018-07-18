#!/usr/bin/env bash

. build_setup.sh

# flexvolume
dazel build //cmd/flexvolume:flexvolume
docker cp dazel:$location/flexvolume/linux_amd64_stripped/flexvolume target/
