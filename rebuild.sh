#!/usr/bin/env bash
set -xe

if [ -f /etc/NIXOS ]
then
    # Bazel doesn't work well with NixOS, so use plan B.
    exec ./rebuild_in_container_without_bazel.sh "$@"
fi

source build-lib.sh

main() {
    build-client $1
    if [ -z "${SKIP_K8S}" ]; then
        build-provisioner
        build-operator
    fi
    build-server
    (cd kubernetes && ./rebuild.sh)
}


main $@

