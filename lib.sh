#!/usr/bin/env bash
set -xe

location=$(realpath .)/bazel-bin/cmd

bazel-with-workspace() {
    cmd=$1
    cmd_path=$2
    platform=${3:-linux_amd64}
    bazel $cmd $cmd_path --platforms=@io_bazel_rules_go//go/toolchain:$platform --workspace_status_command=$(realpath ./version_status.sh)
}

setup-target-dir() {
    mkdir -p target/
}

build-client() {
    OS=$1
    if [ $OS = "Linux" ]; then 
        platform="linux_amd64"
    elif [ $OS = "Darwin" ]; then 
        platform="darwin_amd64" 
    else
        echo "Please enter Linux or Darwin as the first arg"
        return 1
    fi

    output_dir=${platform}_stripped
    rm -rf binaries/$OS || true
    mkdir -p binaries/$OS/

    if [ x$CI_DOCKER_TAG == x ]
    then
        # Non-CI build
        CI_DOCKER_TAG=$VERSION
    fi

    path=${location}/dm/$output_dir/dm
    bazel-with-workspace build //cmd/dm:dm $platform
    # tiny bit hacky - if we're on a mac and compiling for linux the output will be "pure", and vice versa compiling for mac from linux
    if [ ! -d "${location}/dm/${output_dir}" ]; then
        output_dir=${platform}_pure_stripped
    fi
    cp ${location}/dm/$output_dir/dm binaries/$OS/
    return 0
}

build-server() {
    # downloading docker and putting zfs into place is hard in bazel, so cheating using docker :(
    docker build -f cmd/dotmesh-server/Dockerfile -t "base-image-dotmesh" .
    docker save base-image-dotmesh > dotmesh-base.tar

    # skip rebuilding Kubernetes components if not using them
    if [ -z "${SKIP_K8S}" ]; then
        # dind-provisioner (builds a container)
        echo "building dind-provisioner container"
        bazel-with-workspace build //cmd/dotmesh-server/pkg/dind-dynamic-provisioning:dind-dynamic-provisioning
    fi

    # dotmesh-server
    echo "Building dotmesh-server container"
    # TODO serverVersion?
    bazel-with-workspace build //cmd/dotmesh-server:dotmesh-server-img
    #     go build -pkgdir /go/pkg -ldflags "-X main.serverVersion=${VERSION}" -o /target/dotmesh-server
    # allow disabling of registry push
    if [ -z "${NO_PUSH}" ]; then
        echo "pushing images"
        bazel-with-workspace run //cmd/dotmesh-server:dotmesh-server_push
        if [ -z "${SKIP_K8S}" ]; then
            echo "pushing dind provisioner"
            bazel-with-workspace run //cmd/dotmesh-server/pkg/dind-dynamic-provisioning:dind_push
        fi
    fi

}

build-provisioner() {
    bazel-with-workspace build //cmd/dynamic-provisioner:dynamic-provisioner
    if [ -z "${NO_PUSH}" ]; then
        echo "pushing image"
        bazel-with-workspace run //cmd/dynamic-provisioner:provisioner_push
    fi
}

build-operator() {
    # operator (builds container)
    bazel-with-workspace build //cmd/operator:operator

    if [ -z "${NO_PUSH}" ]; then
        echo "pushing image"
        bazel-with-workspace run //cmd/operator:operator_push
    fi

}
