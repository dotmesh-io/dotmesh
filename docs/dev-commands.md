# developing dotmesh via integration tests

## intro

This is the recommended way to develop Dotmesh backend code, where you care
about exercising multi-node or multi-cluster behaviour (e.g. federated
push/pull).

We will use the Dotmesh acceptance test suite, which starts a set of
docker-in-docker environments, one for each node in your cluster and each
cluster in your federation (as configured by the integration test(s) you choose
to run).

The test suite intentionally leaves the last docker-in-docker environments
running so that you can do ad-hoc poking or log/trace viewing after running a
test (using `debug-in-browser.sh`).

This acceptance test suite uses docker-in-docker, kubeadm style. It creates
docker containers which simulate entire computers, each running systemd, and
then uses 'dm cluster init', etc, to set up dotmesh. After the initial setup
and priming of docker images, which takes quite some time, it should take ~60
seconds to spin up a 2 node dotmesh cluster to run a test. It then does not
require internet access.

## System requirements for development

* ~ 8G Memory
* Currently well supported development platforms are NixOS and Ubuntu.

## setting up CI runners

To set up a CI runner on Linux, follow the same instructions as to set up a
development environment for a user named `gitlab-runner` on the machine that is
to become the runner, and then [register a GitLab runner](https://docs.gitlab.com/runner/register/index.html).

Add the `gitlab-runner` user to the docker group, and tell sudo to let
it run things as root without a password, with a line like this in your sudoers file:

```
gitlab-runner ALL=(ALL:ALL) NOPASSWD:ALL
```

MsacOS runners should similarly have a GitLab runner installed, and should
additionally have
[auto-upgrade-docker](https://github.com/dotmesh-io/auto-upgrade-docker)
configured so that we can track breaking changes to Docker for Mac.

When you are asked details in the registration phase, here's what I did for a Ubuntu runner:

```
Please enter the gitlab-ci tags for this runner (comma separated):
fast,ubuntu
Whether to run untagged builds [true/false]:
[false]: true
Whether to lock the Runner to current project [true/false]:
[true]: false
Registering runner... succeeded                     runner=WyJjQ2zg
Please enter the executor: kubernetes, docker, parallels, shell, virtualbox, docker+machine, docker-ssh, ssh, docker-ssh+machine:
shell
```

Add the `gitlab-runner` user's SSH public key to `authorized_hosts` on releases@get.dotmesh.io

Edit  /etc/gitlab-runner/config.toml to set the concurrency to 4 (it's obvious how)

Put the following (sorry) in root's crontab (by e.g. running sudo crontab -e):
```
@hourly bash -c 'for X in `sudo zpool list|grep testpool|cut -f 1 -d " "`; do sudo zpool destroy -f $X; done'
@hourly bash -c 'while [ ! -z "`for C in $(docker ps --format "{{.Names}}" | grep cluster- || true); do docker exec -ti $C bash -c "if test -f /CLEAN_ME_UP; then echo CLEAN; else echo DIRTY; fi"; done|grep DIRTY`" ]; do echo "waiting for tests to complete..."; sleep 1; done; systemctl restart docker'
@reboot rm /dotmesh-test-cleanup.lock
```

## setup - nixos

Use a config like this:

```
boot.supportedFilesystems = [ "zfs" ];
networking.hostId = "cafecafe"; # Make a random one.
networking.hostName = "devmachine"; # Define your hostname.
networking.extraHosts = "127.0.0.1 ${config.networking.hostName}.local";
#nixpkgs.config.allowUnfree = true;
environment.systemPackages = with pkgs; [
#  chromium
#  slack
  wget
  vim
  docker
  docker_compose
  universal-ctags
  mtr
  go
  jq
  tmux
  tmate
  gnumake
  git
  moreutils
];
boot.kernel.sysctl."vm.max_map_count" = 262144; # for elasticsearch
virtualisation.docker = {
  enable = true;
  storageDriver = "overlay2";
  extraOptions = "--insecure-registry ${config.networking.hostName}.local:80";
};
system.activationScripts.binbash = {
  text = "ln -sf /run/current-system/sw/bin/bash /bin/bash";
  deps = [];
};
networking.firewall.enable = false;
```

Then run:
```
sudo nixos-rebuild switch
```

## setup - ubuntu

[Install Docker](https://docs.docker.com/engine/installation/linux/docker-ce/ubuntu/), then put the following docker config in /etc/docker/daemon.json:

```
{
    "storage-driver": "overlay2",
    "insecure-registries": ["$(hostname).local:80"]
}
```

Replacing `$(hostname)` with your hostname, and then `systemctl restart docker`.

Run (as root):
```
apt install zfsutils-linux jq moreutils
echo 'vm.max_map_count=262144' >> /etc/sysctl.conf
sysctl vm.max_map_count=262144
```

Note on golang versions: The current ubuntu LTS is Ubuntu 16.04.3, and the default version of golang on this is go1.6.2. dotmesh requires go1.7 or above.
```
apt-get install golang-1.9
```
This puts binaries in  `/usr/lib/go-1.9/bin`. So you'd need to set symlinks on your PATH to golang binaries

```
ln -s  /usr/lib/go-1.9/bin/go /usr/local/bin/go
ln -s  /usr/lib/go-1.9/bin/gofmt /usr/local/bin/gofmt
```

[Install Docker Compose](https://docs.docker.com/compose/install/).

Add the hostname to the hosts file:

```bash
cat <<EOF >> /etc/hosts
127.0.0.1 $(hostname).local
EOF
```

## setup - debian

[Install Docker](https://docs.docker.com/engine/installation/linux/docker-ce/debian/), then put the following docker config in /etc/docker/daemon.json:

```
{
    "storage-driver": "overlay2",
    "insecure-registries": ["$(hostname).local:80"]
}
```

Replacing `$(hostname)` with your hostname, and then `systemctl restart docker`.

Update /etc/apt/sources.list to include contrib sources for OpenZFS.
```
deb http://ftp.us.debian.org/debian/ stretch main contrib
deb-src http://ftp.us.debian.org/debian/ stretch main contrib

deb http://security.debian.org/debian-security stretch/updates main contrib
deb-src http://security.debian.org/debian-security stretch/updates main contrib

# stretch-updates, previously known as 'volatile'
deb http://ftp.us.debian.org/debian/ stretch-updates main contrib
deb-src http://ftp.us.debian.org/debian/ stretch-updates main contrib
```

Run (as root):
```
apt-get update
apt-get -y install zfsutils-linux jq golang moreutils
echo 'vm.max_map_count=262144' >> /etc/sysctl.conf
sysctl vm.max_map_count=262144
```

[Install Docker Compose](https://docs.docker.com/compose/install/).

Add the hostname to the hosts file:

```bash
cat <<EOF >> /etc/hosts
127.0.0.1 $(hostname).local
EOF
```

## setup - vagrant

First - install vagrant.

Then:

```bash
vagrant up
vagrant ssh
ssh-keygen
# yes to all options
cat ~/.ssh/id_rsa.pub
```

Now paste this key into your github account AND your gitlab account.

Now we login and run the `ubuntu` prepare script:

```bash
vagrant ssh
bash /vagrant/scripts/prepare_vagrant.sh
exit
vagrant ssh
```

Install dep from https://golang.github.io/dep/docs/installation.html

NOTE: you must exit and re-ssh to get the GOPATH to work

Now you can skip directly to ["running tests"](#running-tests).

#### reset vagrant

To reset and bring the vagrant setup up to date:

```bash
vagrant ssh
bash /vagrant/scripts/reset_vagrant.sh
```

#### symlink code

It is possible to mount your local codebase into the vagrant VM so you can re-run the test suite without having to git commit & push.

There can be issues with Vagrant shared folders hence this being a manual step.

```bash
vagrant ssh
cd $GOPATH/src/github.com/dotmesh-io
# might as well keep this
mv dotmesh dotmesh2
ln -s /vagrant dotmesh
# now $GOPATH/src/github.com/dotmesh-io/dotmesh -> /vagrant -> this repo on your host
```

NOTE: using the symlink can drastically slow down docker builds.

You can use this script which copies the latest git hash from your host:

```bash
cd /vagrant
make vagrant.sync
```

## setup

Assuming you have set your GOPATH (e.g. to `$HOME/gocode`):

```
ssh-keygen ## If you haven't already
## Now add your ~/.ssh/id_rsa.pub to your user settings on Gitlab and Github
mkdir -p $GOPATH/src/github.com/dotmesh-io
cd $GOPATH/src/github.com/dotmesh-io
git clone git@gitlab.dotmesh.io:dotmesh/dotmesh
```
Note: The directory structure above is $GOPATH/src/github.com and the cloned repo is from neo.lukemarsden.net (gitlab), this is in preparation for a future migration from gitlab to github.

We're going to create `~/dotmesh-instrumentation` and
`~/discovery.dotmesh.io` directories:

```
cd ~/
git clone git@github.com:dotmesh-io/dotmesh-instrumentation
cd dotmesh-instrumentation
./up.sh secret # where secret is some local password
```

The `dotmesh-instrumentation` pack includes ELK for logging, Zipkin for
tracing, a local registry which is required for the integration tests, and an
etcd-browser which is useful for inspecting the state in your test clusters'
etcd instances.

```
cd ~/
git clone git@github.com:dotmesh-io/discovery.dotmesh.io
cd discovery.dotmesh.io
./start-local.sh
```

The `discovery.dotmesh.io` server provides a discovery service for dotmesh
clusters, we need to run a local one to make the tests work offline.

You have to do some one-off setup and priming of docker images before these
tests will run:

```
cd $GOPATH/src/github.com/dotmesh-io/dotmesh
./prime.sh
```

Install dep from https://golang.github.io/dep/docs/installation.html

## running tests

[Install kubectl](https://kubernetes.io/docs/tasks/tools/install-kubectl/)


To run the test suite, run:

```
cd $GOPATH/src/github.com/dotmesh-io/dotmesh
./prep-tests.sh && ./test.sh -short
```

You can omit `-short` to `test.sh` if you want to run the stress
tests, which take a while. You may need to also pass `-timeout 30m` or
so as well...

To run just an individual set of tests, run:
```
./prep-tests.sh && ./test.sh -run TestTwoSingleNodeClusters
```

To run an individual test, specify `TestTwoSingleNodeClusters/TestName` for
example.

To open a bunch of debug tools (Kibana for logs, Zipkin for traces, and etcd
browsers for each cluster's etcd), run (where 'secret' is the pasword you
specified when you ran `up.sh` in `dotmesh-instrumentation`):

```
ADMIN_PW=secret ./debug-in-browser.sh
```

Note that `debug-in-browser.sh` also dumps goroutine stacks (`*.goroutines`
files) from all running dotmesh instances into the current working directory.
This can be extremely useful for debugging deadlocks: look for suspicious
stacks which indicate that things that are waiting on eachother.

The old UI, if you want to use that, depends on having the Jekyll site running
on `localhost:4000`. The following is a portable way to do that:

```
git clone git@github.com:dotmesh-io/dotmesh-website
cd dotmesh-website
docker run -ti --net=host -v $PWD:/srv/jekyll pwbgl/docker-jekyll-pygments jekyll serve /site
```

#### interacting with etcd

You can use the `etcdctl` dev command to view the contents of the database.

```bash
bash dev.sh etcdctl ls /dotmesh.io
```

**list users**

```bash
bash dev.sh etcdctl ls /dotmesh.io/users
```

**show user**

```bash
bash dev.sh etcdctl get /dotmesh.io/users/00000000-0000-0000-0000-000000000000
```
