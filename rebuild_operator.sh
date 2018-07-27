#!/usr/bin/env bash

set -ex

source build-lib.sh

main() {
    set-defaults
    build-operator
}


main