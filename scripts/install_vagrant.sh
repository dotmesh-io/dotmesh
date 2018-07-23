#!/bin/bash
set -xe

if [ `whoami` != 'root' ]; then
  echo >&2 "You must be root to do this."
  exit 1
fi

apt-get -y update
apt-get install -y docker.io zfsutils-linux jq curl software-properties-common
add-apt-repository -y ppa:gophers/archive
apt-get -y update
apt-get install -y golang-1.10 moreutils pkg-config zip g++ zlib1g-dev unzip python git-core
curl -L -o bazel-installer.sh https://github.com/bazelbuild/bazel/releases/download/0.15.2/bazel-0.15.2-installer-linux-x86_64.sh
chmod +x bazel-installer.sh && ./bazel-installer.sh --user

# make elastic search work
echo 'vm.max_map_count=262144' >> /etc/sysctl.conf
sysctl vm.max_map_count=262144

cat <<EOF > /etc/docker/daemon.json
{
    "storage-driver": "overlay2",
    "insecure-registries": ["$(hostname).local:80"]
}
EOF

cat <<EOF >> /etc/hosts
127.0.0.1 $(hostname).local
EOF

systemctl restart docker
adduser vagrant docker

curl -o /usr/local/bin/docker-compose -L "https://github.com/docker/compose/releases/download/1.15.0/docker-compose-$(uname -s)-$(uname -m)"
chmod +x /usr/local/bin/docker-compose

curl -LO https://storage.googleapis.com/kubernetes-release/release/$(curl -s https://storage.googleapis.com/kubernetes-release/release/stable.txt)/bin/linux/amd64/kubectl
chmod +x ./kubectl
sudo mv ./kubectl /usr/local/bin/kubectl

curl -L -o /usr/local/bin/dep https://github.com/golang/dep/releases/download/v0.4.1/dep-linux-amd64
chmod a+x /usr/local/bin/dep
