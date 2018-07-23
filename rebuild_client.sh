#!/usr/bin/env bash

set -ex

source build-lib.sh

main() {
    build-client $1
}


main $@