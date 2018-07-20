#!/usr/bin/env bash

set -ex

OS=$1
if [ $OS = "Linux" ]; then 
    platform="linux_amd64"
elif [ $OS = "Darwin" ]; then 
    platform="darwin_amd64" 
else
    echo "Please enter Linux or Darwin as the first arg"
    exit 1
fi

location=$(realpath .)/bazel-bin/cmd
output_dir=${platform}_stripped
rm -rf target/$OS || true
mkdir -p target/$OS/

if [ x$CI_DOCKER_TAG == x ]
then
	 # Non-CI build
	 CI_DOCKER_TAG=$VERSION
fi



path=${location}/dm/$output_dir/dm
bazel build //cmd/dm:dm --platforms=@io_bazel_rules_go//go/toolchain:$platform --workspace_status_command=$(realpath ./version_status.sh)
# tiny bit hacky - if we're on a mac and compiling for linux the output will be "pure", and vice versa compiling for mac from linux
if [ ! -d "${location}/dm/${output_dir}" ]; then
    output_dir=${platform}_pure_stripped
fi
cp ${location}/dm/$output_dir/dm target/$OS/
