#!/bin/bash
set -xe

if [ `whoami` != 'vagrant' ]; then
  echo >&2 "You must be the vagrant user to do this."
  exit 1
fi

docker rm -f $(docker ps -aq)

cd $HOME/dotmesh-instrumentation
git pull
./up.sh secret

cd $HOME/discovery.dotmesh.io
git pull
./start-local.sh

cd $GOPATH/src/github.com/dotmesh-io/dotmesh
git pull
if [ -z "$SKIP_K8S" ]; then
  ./prime.sh
else
  ./prime-docker.sh
fi
