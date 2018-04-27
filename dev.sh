#!/usr/bin/env bash -e
#
# scripts to manage the developers local installation of dotmesh
#

set -e

export DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

export DATABASE_ID=${DATABASE_ID:=""}
export DOTMESH_HOME=${DOTMESH_HOME:="~/.dotmesh"}
export GOOS=${GOOS:="darwin"}

export CI_DOCKER_SERVER_IMAGE=${CI_DOCKER_SERVER_IMAGE:=$(hostname).local:80/dotmesh/dotmesh-server}
export DEV_NETWORK_NAME=${DEV_NETWORK_NAME:="dotmesh-dev"}
export VAGRANT_HOME=${VAGRANT_HOME:="/vagrant"}
export GOOS=${GOOS:="darwin"}
export VAGRANT_GOPATH=${VAGRANT_GOPATH:="$GOPATH/src/github.com/dotmesh-io/dotmesh"}

# Use the binary that we just built, rather than one that's lying around on
# your system.
export DM="binaries/$(uname -s)/dm"

function build() {
  export NO_PUSH=1
  export SKIP_K8S=1
  cli-build
  cluster-build
}

function cli-build() {
  echo "building dotmesh CLI binary"
  bash rebuild.sh $GOOS
  echo
  echo "dm binary created - copy it to /usr/local/bin with this command:"
  echo
  echo "sudo cp -f ./binaries/\$(uname -s |tr \"[:upper:]\" \"[:lower:]\")/dm /usr/local/bin"
}

function cluster-build() {
  cd "${DIR}/cmd/dotmesh-server" && IMAGE="${CI_DOCKER_SERVER_IMAGE}" NO_PUSH=1 bash rebuild.sh
}

function cluster-start() {
  echo "creating cluster using: ${CI_DOCKER_SERVER_IMAGE}"
  $DM cluster init \
    --image ${CI_DOCKER_SERVER_IMAGE} \
    --offline
  sleep 2
  docker network create $DEV_NETWORK_NAME &>/dev/null || true
  docker network connect $DEV_NETWORK_NAME $DOTMESH_SERVER_NAME
}

function cluster-stop() {
  docker rm -f dotmesh-server-inner
  docker rm -f dotmesh-server
  docker rm -f dotmesh-etcd
}

function cluster-upgrade() {
  echo "upgrading cluster using ${CI_DOCKER_SERVER_IMAGE}"
  $DM cluster upgrade \
    --image ${CI_DOCKER_SERVER_IMAGE} \
    --offline
  sleep 2
  docker network create $DEV_NETWORK_NAME &>/dev/null || true
  docker network connect $DEV_NETWORK_NAME $DOTMESH_SERVER_NAME
}

function reset() {
  $DM cluster reset || true
}

function etcdctl() {
  docker exec -ti dotmesh-etcd etcdctl \
    --cert-file=/pki/apiserver.pem \
    --key-file=/pki/apiserver-key.pem \
    --ca-file=/pki/ca.pem \
    --endpoints https://127.0.0.1:42379 "$@"
}

# print the api key for the admin user
function admin-api-key() {
  cat $HOME/.dotmesh/config  | jq -r '.Remotes.local.ApiKey'
}

function vagrant-sync() {
  githash=$(cd $VAGRANT_HOME && git rev-parse HEAD)  
  rm -rf $VAGRANT_GOPATH/.git
  cp -r /vagrant/.git $VAGRANT_GOPATH
  (cd $VAGRANT_GOPATH && git reset --hard $githash)
}

function vagrant-list-changed-files() {
  files=$(git status | grep "modified:" | awk '{print $2}')
  echo $files
}

function vagrant-copy-changed-files() {
  for file in $CHANGED_FILES; do
    copypcommand="cp -r /vagrant/$file /home/vagrant/gocode/src/github.com/dotmesh-io/dotmesh/$file"
    echo $copypcommand
    eval $copypcommand
  done
}

function vagrant-prepare() {
  vagrant-sync
  (cd $VAGRANT_GOPATH && ./prep-tests.sh)
}

function vagrant-test() {
  local SINGLE_TEST=""
  if [ -n "$TEST_NAME" ]; then
    SINGLE_TEST=" -run $TEST_NAME"
  fi
  (cd $VAGRANT_GOPATH && ./test.sh $SINGLE_TEST)
}

function vagrant-gopath() {
  echo $VAGRANT_GOPATH
}

function vagrant-tunnel() {
  local node="$1"
  if [ -z "$node" ]; then
    echo >&2 "usage vagrant-tunnel <containerid>"
    exit 1
  fi
  local ip=$(docker inspect -f '{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}' $node)
  echo http://172.17.1.178:8080
  socat tcp-listen:8080,reuseaddr,fork tcp:$ip:8080
}

eval "$@"
