#!/usr/bin/env bash

set -ex

if [ -f /etc/NIXOS ]
then
    # Bazel doesn't work well with NixOS, so use plan B.
    exec ./rebuild_in_container_without_bazel.sh "$@"
fi

source build-lib.sh

main() {
    set-defaults
    build-server
}

main
