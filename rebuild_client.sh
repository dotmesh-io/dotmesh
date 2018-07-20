#!/usr/bin/env bash

set -ex

OS=$1
if [ $OS = "Linux" ]; then 
    platform="linux_amd64"
    output_dir=${platform}_stripped
elif [ $OS = "Darwin" ]; then 
    platform="darwin" 
    output_dir=${platform}_stripped
else
    echo "Please enter Linux or Darwin as the first arg"
    exit 1
fi

mkdir -p target/$OS/

if [ x$CI_DOCKER_TAG == x ]
then
	 # Non-CI build
	 CI_DOCKER_TAG=$VERSION
fi


location=$(realpath .)/bazel-bin/cmd
bazel build //cmd/dm:dm --platforms=@io_bazel_rules_go//go/toolchain:$platform --workspace_status_command=$(realpath ./version_status.sh)
cp ${location}/dm/$output_dir/dm target/$OS/
