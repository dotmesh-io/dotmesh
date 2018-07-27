#!/usr/bin/env bash
set -xe

source build-lib.sh

pull-then-push() {
    img=$1
    docker pull $OLD_REG/$OLD_REPO/$img:$CI_DOCKER_TAG
    docker tag $OLD_REG/$OLD_REPO/$img:$CI_DOCKER_TAG $CI_REGISTRY/$CI_REPOSITORY/$img:$CI_DOCKER_TAG 
    docker push $CI_REGISTRY/$CI_REPOSITORY/$img:$CI_DOCKER_TAG 
}
main() {
    export OLD_REG=$CI_REGISTRY
    export OLD_REPO=$CI_REPOSITORY
    export CI_REGISTRY=$1
    export CI_REPOSITORY=$2
    pull-then-push dotmesh-server
    # do a full rebuild on operator because it needs to know the server image link
    build-operator
    pull-then-push dotmesh-operator
    pull-then-push dotmesh-dynamic-provisioner
}

main $@
