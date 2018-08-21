#!/bin/bash
set -xe


export GITHUB_HOST=${GITHUB_HOST:="github.com"}
export GITHUB_ORG=${GITHUB_ORG:="dotmesh-io"}
export GITHUB_REPO=${GITHUB_REPO:="dotmesh"}
export INSTRUMENTATION_REPO=${INSTRUMENTATION_REPO:="dotmesh-instrumentation"}
export DISCOVERY_REPO=${DISCOVERY_REPO:="discovery.dotmesh.io"}

if [ `whoami` != 'vagrant' ]; then
  echo >&2 "You must be the vagrant user to do this."
  exit 1
fi

mkdir -p $HOME/.ssh
cat <<EOF > $HOME/.ssh/config
Host $GITHUB_HOST
  StrictHostKeyChecking no
  UserKnownHostsFile=/dev/null
EOF

if [ -z "${GOPATH}" ]; then
  sudo ln -s  /usr/lib/go-1.10/bin/go /usr/local/bin/go
  sudo ln -s  /usr/lib/go-1.10/bin/gofmt /usr/local/bin/gofmt
  export GOPATH=/home/vagrant/gocode
  export PATH=/home/vagrant/bin:$PATH
  echo "export GOPATH=${GOPATH}" >> $HOME/.bash_profile
  echo "export PATH=\$HOME/bin:\$PATH" >> $HOME/.bash_profile
  echo "export DM_FOLDER=\$GOPATH/src/github.com/dotmesh-io/dotmesh" >> $HOME/.bash_profile
fi

mkdir -p $GOPATH

if [ ! -d "$GOPATH/src/$GITHUB_HOST/$GITHUB_ORG/$GITHUB_REPO" ]; then
  mkdir -p $GOPATH/src/$GITHUB_HOST/$GITHUB_ORG
  cd $GOPATH/src/$GITHUB_HOST/$GITHUB_ORG
  git clone git@$GITHUB_HOST:$GITHUB_ORG/$GITHUB_REPO
fi

if [ ! -d "$HOME/$INSTRUMENTATION_REPO" ]; then
  cd $HOME/
  git clone git@$GITHUB_HOST:$GITHUB_ORG/$INSTRUMENTATION_REPO
  cd $INSTRUMENTATION_REPO
fi

cd $HOME/$INSTRUMENTATION_REPO
./up.sh

if [ ! -d "$HOME/$DISCOVERY_REPO" ]; then
  cd $HOME/
  git clone git@$GITHUB_HOST:$GITHUB_ORG/$DISCOVERY_REPO
fi

cd $HOME/$DISCOVERY_REPO
./start-local.sh

cd $GOPATH/src/$GITHUB_HOST/$GITHUB_ORG/$GITHUB_REPO
./prime.sh

# start samba for sharing between host and vm
docker run -d -p 139:139 -p 445:445 --name samba --restart always -v $GOPATH/src/$GITHUB_HOST/$GITHUB_ORG:/mount dperson/samba -u "admin;password" -s "vagrantshare;/mount;yes;no;no;admin;admin;admin;comment"