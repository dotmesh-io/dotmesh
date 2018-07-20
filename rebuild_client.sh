#!/usr/bin/env bash

set -ex

source lib.sh

main() {
    setup-env
    build-client $1
}


main $@