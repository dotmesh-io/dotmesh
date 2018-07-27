#!/usr/bin/env bash

set -ex

source build-lib.sh

main() {
    set-defaults
    build-client $1
}


main $@