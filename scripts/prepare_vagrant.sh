#!/bin/bash
set -xe


export GITHUB_ORG=${GITHUB_ORG:="dotmesh-io"}
export GITHUB_REPO=${GITHUB_REPO:="dotmesh"}
export GITLAB_HOST=${GITLAB_HOST:="neo.lukemarsden.net"}
export GITLAB_ORG=${GITLAB_ORG:="dotmesh"}
export GITLAB_REPO=${GITLAB_REPO:="dotmesh"}
export INSTRUMENTATION_REPO=${INSTRUMENTATION_REPO:="dotmesh-instrumentation"}
export DISCOVERY_REPO=${DISCOVERY_REPO:="discovery.dotmesh.io"}

if [ `whoami` != 'vagrant' ]; then
  echo >&2 "You must be the vagrant user to do this."
  exit 1
fi

mkdir -p $HOME/.ssh
cat <<EOF > $HOME/.ssh/config
Host github.com
  StrictHostKeyChecking no
  UserKnownHostsFile=/dev/null
Host $GITLAB_HOST
  StrictHostKeyChecking no
  UserKnownHostsFile=/dev/null
EOF

if [ -z "${GOPATH}" ]; then
  export GOPATH=/home/vagrant/gocode
  export PATH=$PATH:/usr/lib/go-1.8/bin
  echo "export GOPATH=${GOPATH}" >> $HOME/.bash_profile
  echo "export PATH=\$PATH:/usr/lib/go-1.8/bin:$GOPATH/bin" >> $HOME/.bash_profile
fi

mkdir -p $GOPATH

if [ ! -d "$GOPATH/src/github.com/$GITHUB_ORG/$GITHUB_REPO" ]; then
  mkdir -p $GOPATH/src/github.com/$GITHUB_ORG
  cd $GOPATH/src/github.com/$GITHUB_ORG
  git clone git@$GITLAB_HOST:$GITLAB_ORG/$GITLAB_REPO
fi

if [ ! -d "$HOME/$INSTRUMENTATION_REPO" ]; then
  cd $HOME/
  git clone git@github.com:$GITHUB_ORG/$INSTRUMENTATION_REPO
  cd $INSTRUMENTATION_REPO
fi

cd $HOME/$INSTRUMENTATION_REPO
./up.sh secret # where secret is some local password

if [ ! -d "$HOME/$DISCOVERY_REPO" ]; then
  cd $HOME/
  git clone git@github.com:$GITHUB_ORG/$DISCOVERY_REPO
fi

cd $HOME/$DISCOVERY_REPO
./start-local.sh

cd $GOPATH/src/github.com/$GITHUB_ORG/$GITHUB_REPO

./prime.sh
go get -u github.com/golang/dep/cmd/dep
