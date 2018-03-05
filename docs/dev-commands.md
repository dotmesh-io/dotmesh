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

You can pass `SKIP_FRONTEND=true` to `prep-tests.sh` if you want to trade having
a working frontend for slightly faster builds (this will break `TestFrontend`).

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

If you are not wanting to test the Kubernetes integration (which can take a while to build Docker images) - set this variable before running `./prep-tests.sh`

```
export NO_K8S=1
```

# single server local dev

How to develop a single dotmesh server, frontend and CLI locally with Docker.

*These instructions are most useful for developing the Javascript frontend.*

## building images

Before you begin, upgrade your Docker then build the required images:

```bash
make build
```

This will:

 * build the cluster container images
 * build the dm CLI and install it to `/usr/local/bin/dm`
 * build the frontend development image

You can run the three build stages seperately:

```bash
make cluster.build
make frontend.build
make cli.build
```

NOTE: you will need to copy the `dm` binary as instructed by the output of the `make cli.build` command - make sure you do this


## run the stack

#### start cluster
First we bring up a dotmesh cluster:

```bash
make cluster.start
```

This will start an etcd and 2 dotmesh containers - `docker ps` will show this.

If this produces errors - you can use this command to reset and try again:

```bash
make reset
```

#### start cluster with billing

You need to create a `credentials.env` file (it's .gitignored) looking like this:

```
STRIPE_PUBLIC_KEY=pk_test_XXX
STRIPE_SECRET_KEY=sk_test_XXX
```

Then activate the `LOCAL_BILLING` variable before starting the cluster as normal:

```bash
export LOCAL_BILLING=1
make cluster.start
```

Then we start the billing service:

```bash
make billing.start
```

Or if we want a CLI inside the billing container (so we can `node src/index.js` manually):

```bash
make billing.dev
```

The frontend will now activate the payment section and all stripe requests will go via the billing container.

#### testing stripe web hooks

If you want to test a full loop with incoming Stripe webhooks:

 * get your public IP from [whatismyip.com](http://whatismyip.com)
 * login to the Stripe control panel -> api -> [webhooks](https://dashboard.stripe.com/account/webhooks)
 * add the url and make sure you are adding it to the **test** mode
 * copy the signature secret for the endpoint you added and add it to `credentials.env`:
   * `STRIPE_SIGNATURE_SECRET=...`
 * restart the billing server

Now you can use the app locally with the example card (`4242 4242 4242 4242`) and payment web-hooks will arrive to the local billing service.


#### following billing service logs

A useful trick when working with the billing service interactively (i.e. using `make billing.dev`) is to pipe the logs via `jq`:

```bash
$ make billing.dev
root$ apt-get install -y jq
root$ node src/index.js | jq .
```

This will print the Stripe events and other information in a much more human friendly way.


#### start communications service

You need to create a `credentials.env` file (it's .gitignored) looking like this:

```
MAILGUN_API_KEY=key-XXX
MAILGUN_DOMAIN=mail.dotmesh.io
```

Start the communications service:

```bash
make communications.start
```

Or in dev mode:

```bash
make communications.dev
```

Then activate the `LOCAL_COMMUNICATIONS` variable before starting the billing service as normal:

```bash
export LOCAL_COMMUNICATIONS=1
make billing.start
```

This will hook up the billing service to start sending requests to the communications service.

This means you can run the billing service without running a communications service at all.

#### testing the communications service

By default - the communications service is not `activated` - this means it will recieve requests but not send any emails or texts.

Instead - it will append all email requests to `/tmp/emails.json` - this can then be used to analyse after running your requests:

```bash
$ docker cp dotmesh-communications:/tmp/emails.json emails.json
```

#### activating the communications service

To activate the communications service so it will actually send emails:

```bash
export ACTIVATE_COMMUNICATIONS=1
make communications.start
```

This is the production trim and requests will not be written to `/tmp/emails.json`


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

#### start frontend
Then we bring up the frontend container (which proxies back to the cluster for api requests):

```bash
make frontend.start
```

To attach to the frontend logs:

```bash
make frontend.logs
```

Now you should be able to open the app in your browser:

```bash
open http://localhost:8080
```

To view the new UI:

```bash
open http://localhost:8080/ui
```

If you want to see the cluster server directly - you can:

```bash
open http://localhost:6969
```

#### frontend CLI

Sometimes it's useful to have the frontend container hooked up but with a bash prompt:

```bash
make frontend.dev
yarn run watch
```

#### linking templatestack

The `template-ui` and `template-tools` npm modules are used in the UI and to iterate quickly it can be useful to have these linked up to the hot reloading.

To do this - you first need to clone https://github.com/binocarlos/templatestack.git to the same folder as dotmesh then:

```bash
make frontend.link
yarn run watch
```

Now - any changes made to `templatestack/template-ui` will hot-reload.  Changes made to template-ui must be published to npm!

#### reset & boot errors

If anything happens which results in the cluster not being able to boot - usually the solution is:

```bash
make reset
```

which does:

```bash
dm cluster reset
```

## stop the stack

To stop a running stack - use these commands:

```bash
make cluster.stop
make frontend.stop
make reset
```

We currently need to reset the cluster each time we stop - this means you will have to re-create your user account when you restart the stack.

Normally this is not too painful because you can rebuild the server code and `upgrade` the cluster (read below).

TODO: allow existing admin password / possibly PKI key data to be used when bringing up the cluster
TODO: have user defined fixture files than can quickly create user accounts for local dev

## changing code

Here is how you would edit code for the 3 main sections of dotmesh:

 * [server](cmd/dotmesh-server) - container name = `dotmesh-server`
 * [cli](cmd/dm) - binary location = `/usr/local/bin/dm`
 * [frontend](frontend) - container name = `dotmesh-frontend`

#### server

Once you have edited the [server code](cmd/dotmesh-server) - run the build script:

```bash
make cluster.build
```

Then we run the `upgrade` script which will replace in place the server container with our new image:

```bash
make cluster.upgrade
```

#### adding go dependencies

If you have included a new go dependency in your code - follow these steps:

 * symlink the current repo into $GOPATH/src/github.com/dotmesh-io/dotmesh
 * cd $GOPATH/src/github.com/dotmesh-io/dotmesh/cmd/{dotmesh-server,dm}
 * dep ensure
 * # now commit the vendor folder and Gopkg.{lock|toml}

#### cli

Once you have edited the [cli code](cmd/dm) - run the build script:

```bash
make cli.build
```

This will build the go code in a container and output it to `binaries/$GOOS`.

We then move the binary to use it:

```bash
sudo mv -f binaries/darwin/dm /usr/local/bin/dm
sudo chmod +x /usr/local/bin/dm
```

#### frontend

The frontend is built using a [webpack config](frontend/webpack.config.js) and the local code is mounted as a volume which automatically triggers a rebuild when you save a file.

The code is mounted with a [webpack-hot-middleware](https://github.com/glenjamin/webpack-hot-middleware) server so if you are editing React components they should auto-reload in the browser.

If you are editing sagas, CSS or any of the non-visual part of the frontend, you will have to reload the browser.

#### rebuilding frontend image

There are times when you will need to rebuild the frontend image for example if you are adding an npm module.

First, stop the frontend server:

```bash
make frontend.stop
```

Then - use yarn to add the module:

```bash
cd frontend
yarn add my-new-kool-aid
cd ..
```

Build and start the frontend image:

```bash
make frontend.build
make frontend.start
```

You can `docker exec -ti dotmesh-frontend bash` to get a CLI inside the frontend container to run any other commands.

#### changing the help markdown

If you change any of the `.md` files inside `frontend/help` - you will need to restart the frontend container.

The markdown is automatically built when the frontend dev/release server is run - there is no hot-reloading on the help.

This will generate the following file: `frontend/src/ui/help.json` which is used to generate the help pages in React - this file is `.gitignored` and should be generated automatically as part of the normal dev/release commands.

#### using variables in the help markdown

You can use dynamic variables in the help markdown files using the following format: `${VAR_NAME}`

These are populated client side and the following variables are available:

 * `USER_NAME` - user name of current user
 * `SERVER_NAME` - url of current dotmesh installation

These variables are defined in [frontend/src/ui/selectors.js](frontend/src/ui/selectors.js) -> `help.variables`

#### building frontend production code

To build the production distribution of the frontend code:

```bash
make frontend.dist
```

This will create output in `frontend/dist` which can be copied into the server container for production.

The frontend `dist` folder is merged into the dotmesh-server image in the `merge` CI job.

## running production mode

To run the frontend code in production mode (i.e. static files inside the server) - do the following:

```bash
make prod
```

This will:

```bash
make frontend.build
make frontend.dist
make cluster.build
make cluster.prodbuild
make cluster.start
```

and end up with the same as `cluster.start` but with the frontend code built into the server.

The difference in this mode is you need to hit `localhost:6969` to see it in the browser.

## running frontend tests

It is useful to run the frontend tests against a running hot-reloading development env.

First - startup chromedriver and build the test image.

```bash
make frontend.test.build # only needed once
make chromedriver.start
```

Then install [gotty](https://github.com/yudai/gotty) - this is used to run `dm` commands from the browser.

Run the gotty server:

```bash
$ make gotty.start
```

Export `GOTTY_HOST` to be an IP address that Docker containers can speak to.

If on Linux you can use the docker bridge - if on OSX - use the local ip:

```bash
$ export GOTTY_HOST=$(ipconfig getifaddr en0)
```

Then - as the frontend is rebuilding as you make changes - you can re-run the test suite:

```bash
$ make frontend.test
```

If you want to run a single test - export `TEST_NAME`:

```bash
$ export TEST_NAME=specs/endtoend.js
```

Videos & screenshots are produced after each test run - they live in `frontend/.media`

If you want the tests to proceed faster but with a less human viewable nature:

```bash
$ export QUICK=1
$ make frontend.test
```

## running frontend tests with billing

If you have activated the billing server and want to run the frontend tests - you must do the following:

```bash
$ export BILLING_URL=http://billing/api/v1
$ make cluster.upgrade
$ make frontend.test
```

Because in the tests the browser runs inside a container - the billing url can no longer be `localhost` - this trick means we can now see the billing server from inside the chromedriver container.

The acceptance tests make use of this also (automatically).

#### production trim

If you are running the production trim (where the frontend code is burnt into the server):

```bash
make chromedriver.start.prod
make frontend.test.prod
```
