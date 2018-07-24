#!/usr/bin/env bash
set -xe

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

