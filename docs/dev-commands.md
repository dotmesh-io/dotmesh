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
running so that you can do ad-hoc poking or log viewing after running a
test.

This acceptance test suite uses docker-in-docker, kubeadm style. It creates
docker containers which simulate entire computers, each running systemd, and
then uses 'dm cluster init', etc, to set up dotmesh. After the initial setup
and priming of docker images, which takes quite some time, it should take ~60
seconds to spin up a 2 node dotmesh cluster to run a test. It then does not
require internet access.

## System requirements for development

* ~ 8G Memory
* Currently well supported development platforms are NixOS and Ubuntu.

## Setup - nixos

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

## Setup - ubuntu (18.04)

Follow these steps to get setup for Ubuntu 18.04:

```bash
# install go
snap install go --classic

# install docker
apt-get update
apt-get install     apt-transport-https     ca-certificates     curl     software-properties-common
curl -fsSL https://download.docker.com/linux/ubuntu/gpg | sudo apt-key add -
apt-key fingerprint 0EBFCD88
add-apt-repository    "deb [arch=amd64] https://download.docker.com/linux/ubuntu \
   $(lsb_release -cs) \
   stable"
apt-get update
apt-get install docker-ce

# configure docker
cat << EOT > /etc/docker/daemon.json
{
    "storage-driver": "overlay2",
    "insecure-registries": ["$(hostname).local:80"]
}
EOT
systemctl restart docker

# install packages
apt install zfsutils-linux jq moreutils
echo 'vm.max_map_count=262144' >> /etc/sysctl.conf
sysctl vm.max_map_count=262144

# install docker-compose
sudo curl -L https://github.com/docker/compose/releases/download/1.22.0/docker-compose-$(uname -s)-$(uname -m) -o /usr/local/bin/docker-compose
sudo chmod +x /usr/local/bin/docker-compose

# update hosts
cat <<EOF >> /etc/hosts
127.0.0.1 $(hostname).local
EOF

# install bazel
apt-get -y install pkg-config zip g++ zlib1g-dev unzip python git-core
curl -L -o bazel-installer.sh https://github.com/bazelbuild/bazel/releases/download/0.15.2/bazel-0.15.2-installer-linux-x86_64.sh
chmod +x bazel-installer.sh && ./bazel-installer.sh --user
```

## Setup - debian

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
apt-get -y install zfsutils-linux jq golang moreutils python3-pip
echo 'vm.max_map_count=262144' >> /etc/sysctl.conf
sysctl vm.max_map_count=262144
pip3 install dazel==0.0.36
```

[Install Docker Compose](https://docs.docker.com/compose/install/).

Add the hostname to the hosts file:

```bash
cat <<EOF >> /etc/hosts
127.0.0.1 $(hostname).local
EOF
```

## Setup - vagrant

First - install vagrant. e.g `brew install vagrant`. If you haven't already, you likely need a linux VM driver of some sort, e.g `brew install virtualbox`.

Then, from wherever you cloned `dotmesh`:

```bash
vagrant up 
vagrant ssh
ssh-keygen
# yes to all options
cat ~/.ssh/id_rsa.pub
```

Now paste this key into your github account. (Your profile pic -> settings -> SSH & GPG keys -> add new)

Now we login and run the `ubuntu` prepare script:

```bash
vagrant ssh
bash /vagrant/scripts/prepare_vagrant.sh 
exit
vagrant ssh
```
On first run `vagrant up` will probably take 5-10 minutes, `prepare_vagrant.sh` will probably take 10-15 minutes.

NOTE: you must exit and re-ssh to get the GOPATH to work

You may need to update your docker version - see `scripts/update-docker.sh` 
Now you can skip directly to ["running tests"](#running-tests).

#### reset vagrant

To reset and bring the vagrant setup up to date:

```bash
vagrant ssh
bash /vagrant/scripts/reset_vagrant.sh
```

#### Working from your host
**Optional**: If you would like to work on code from your local machine (i.e not inside a VM), follow this section. Alternatively, you can just change code inside the vagrant VM and commit/push from inside it.
```
open smb://admin:password@172.17.1.178/vagrantshare
```
This should open a shared network drive which allows you to edit files in the dotmesh-io folder of your vagrant machine go path from your host machine.


## generic setup instructions

Assuming you have set your GOPATH (e.g. to `$HOME/gocode`):

```
ssh-keygen ## If you haven't already
## Now add your ~/.ssh/id_rsa.pub to your user settings on Gitlab and Github
mkdir -p $GOPATH/src/github.com/dotmesh-io
cd $GOPATH/src/github.com/dotmesh-io
git clone git@github.com:dotmesh-io/dotmesh
```

We're going to create `~/dotmesh-instrumentation` and
`~/discovery.dotmesh.io` directories:

```
cd ~/
git clone git@github.com:dotmesh-io/dotmesh-instrumentation
cd dotmesh-instrumentation
./up.sh
```

The `dotmesh-instrumentation` pack includes a local registry which is required
for the integration tests.

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

**NOTE** if you are installing a runner on Ubuntu Bionic - tag the runner as `ubuntu-bionic` rather than just `ubuntu`

Add the `gitlab-runner` user's SSH public key to `authorized_hosts` on releases@get.dotmesh.io

Edit  /etc/gitlab-runner/config.toml to set the concurrency to 4 (it's obvious how)

#### setting up a Bionic runner from scratch

There are the steps to follow to get a Bionic runner setup - you will need a login to github and gitlab under the dotmesh namespace.

First - as root, login to the server, then follow these steps:

First - run through the `Setup - ubuntu (18.04)` section above - then:

```bash
# make a gitlab-runner user
adduser \
   --disabled-password \
   --home /home/gitlab-runner \
   --gecos "" \
   gitlab-runner


# add gitlab-runner to sudoers
chmod 0660 /etc/sudoers
echo 'gitlab-runner ALL=(ALL:ALL) NOPASSWD:ALL' >> /etc/sudoers
chmod 0440 /etc/sudoers

# add gitlab-runner to the docker group
usermod -a -G docker gitlab-runner

# setup the gitlab-runner user
su - gitlab-runner
cd ~
mkdir -p gocode/bin

# setup GOPATH
echo export GOPATH=\"\$HOME/go\" >> ~/.bash_profile
echo export PATH=\"\$PATH:\$GOPATH/bin\" >> ~/.bash_profile

# make an ssh key
ssh-keygen

# print public key for next step
cat ~/.ssh/id_rsa.pub
```

Now you must add the public key printed in the last step to your github and gitlab SSH keys thus giving the gitlab-runner user access to our private repos.

Also - copy the `/home/gitlab-runner/.ssh/authorized_keys` file from gitlab-runner.dotmesh.io and copy it into `/home/gitlab-runner/.ssh/authorized_keys` on the new runner (don't forgot to `chmod 0600 /home/gitlab-runner/.ssh/authorized_keys`)

Then continue:

```bash
exit
exit
# you should now have accesss because we added authorized_keys
ssh gitlab-runner@<HOSTNAME-OF-RUNNER>
su - gitlab-runner
# clone the dotmesh repo
mkdir -p $GOPATH/src/github.com/dotmesh-io
cd $GOPATH/src/github.com/dotmesh-io
git clone git@github.com:dotmesh-io/dotmesh.git
cd dotmesh
./prime.sh

# clone & setup the dotmesh-instrumentation repo
cd ~/
git clone git@github.com:dotmesh-io/dotmesh-instrumentation
cd dotmesh-instrumentation
./up.sh

# clone & setup the discovery.dotmesh.io repo
cd ~/
git clone git@github.com:dotmesh-io/discovery.dotmesh.io
cd discovery.dotmesh.io
./start-local.sh

# install gitlab runner
sudo wget -O /usr/local/bin/gitlab-runner https://gitlab-runner-downloads.s3.amazonaws.com/latest/binaries/gitlab-runner-linux-amd64
sudo chmod +x /usr/local/bin/gitlab-runner
sudo gitlab-runner install --user=gitlab-runner --working-directory=/home/gitlab-runner
```

Then - edit the `/etc/gitlab-runner/config.toml` file (as root) as change the `concurrent` setting to 4.

Visit the [gitlab runner admin page](https://gitlab.dotmesh.com/admin/runners) and make a note of:

 * the runner token
 * the URL of coordinator

```bash
sudo gitlab-runner start
sudo gitlab-runner register
```

Here are the values to enter:

 * `tags` = `fast,ubuntu-bionic`
 * `executor` = `shell`

Example:

```
gitlab-runner@gitlab-dawdler:~$ sudo gitlab-runner register
Running in system-mode.

Please enter the gitlab-ci coordinator URL (e.g. https://gitlab.com/):
https://gitlab.dotmesh.com/
Please enter the gitlab-ci token for this runner:
XXX
Please enter the gitlab-ci description for this runner:
[gitlab-dawdler]:
Please enter the gitlab-ci tags for this runner (comma separated):
fast,ubuntu-bionic
Registering runner... succeeded                     runner=WyJjQ2zg
Please enter the executor: docker-ssh, parallels, shell, ssh, virtualbox, kubernetes, docker, docker-ssh+machine, docker+machine:
shell
Runner registered successfully. Feel free to start it, but if it's running already the config should be automatically reloaded!
```

Finally - edit each runner on the [gitlab runner admin page](https://gitlab.dotmesh.com/admin/runners) and untick the `Lock to current projects` checkbox.

## cleanup code - for CI runners and for local dev machines

Put the following (sorry) in root's crontab (by e.g. running sudo crontab -e):
```
@reboot rm /dotmesh-test-cleanup.lock
@daily bash -c 'echo y | docker system prune -a --volumes; find /dotmesh-test-pools -ctime +1 -delete'
```

## Checking out etcd contents in smoke runners



```bash
docker exec -it [container ID] etcdctl --cert-file=/pki/apiserver.pem --key-file=/pki/apiserver-key.pem --ca-file=/pki/ca.pem --endpoint "https://127.0.0.1:42379" ls /dotmesh.io/filesystems
```