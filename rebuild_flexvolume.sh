#!/usr/bin/env bash

. build_setup.sh

# flexvolume
location=$(realpath .)/bazel-bin/cmd
bazel build //cmd/flexvolume:flexvolume
cp $location/flexvolume/linux_amd64_stripped/flexvolume target/
