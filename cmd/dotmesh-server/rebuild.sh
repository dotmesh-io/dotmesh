#!/usr/bin/env bash

# NOTE - I have split these apart so there is an option to rebuild only the
# dotmesh server without building the k8s binaries
# this yields a big time saving when only developing the frontend stack
# without k8s - export DISABLE_K8S=1 disables the k8s build
set -xe

CI_DOCKER_SERVER_IMAGE=${CI_DOCKER_SERVER_IMAGE:=$(hostname).local:80/dotmesh/dotmesh-server:latest}
CI_DOCKER_PROVISIONER_IMAGE=${CI_DOCKER_PROVISIONER_IMAGE:=$(hostname).local:80/dotmesh/dotmesh-dynamic-provisioner:latest}
VERSION="$(cd ../versioner && go run versioner.go)"

mkdir -p target

echo "building image: dotmesh-builder"
docker build --build-arg VERSION="${VERSION}" -f Dockerfile.build -t dotmesh-builder .

# docker
echo "creating container: dotmesh-builder-docker"
docker rm -f dotmesh-builder-docker || true
docker create \
  --name dotmesh-builder-docker \
  dotmesh-builder
echo "copy binary: /target/docker"
docker cp dotmesh-builder-docker:/target/docker target/
docker rm -f dotmesh-builder-docker

# skip rebuilding Kubernetes components if not using them
if [ -z "${SKIP_K8S}" ]; then

  # flexvolume
  echo "creating container: dotmesh-builder-flexvolume"
  docker rm -f dotmesh-builder-flexvolume || true
  docker run \
    --name dotmesh-builder-flexvolume \
    -e GOPATH=/go \
    -w /go/src/github.com/dotmesh-io/dotmesh/cmd/dotmesh-server/pkg/flexvolume \
    dotmesh-builder \
    /usr/lib/go-1.7/bin/go build -o /target/flexvolume
  echo "copy binary: /target/flexvolume"
  docker cp dotmesh-builder-flexvolume:/target/flexvolume target/
  docker rm -f dotmesh-builder-flexvolume

  # dm-provisioner
  echo "creating container: dotmesh-builder-dm-provisioner"
  docker rm -f dotmesh-builder-dm-provisioner || true
  docker run \
    --name dotmesh-builder-dm-provisioner \
    -e GOPATH=/go \
    -e CGO_ENABLED=0 \
    -w /go/src/github.com/dotmesh-io/dotmesh/cmd/dotmesh-server/pkg/dynamic-provisioning \
    dotmesh-builder \
    /usr/lib/go-1.7/bin/go build -a -ldflags '-extldflags "-static"' -o /target/dm-provisioner .
  echo "copy binary: /target/dm-provisioner"
  docker cp dotmesh-builder-dm-provisioner:/target/dm-provisioner target/
  docker rm -f dotmesh-builder-dm-provisioner

  echo "building image: ${CI_DOCKER_PROVISIONER_IMAGE}"
  docker build -f pkg/dynamic-provisioning/Dockerfile -t "${CI_DOCKER_PROVISIONER_IMAGE}" .
fi

# dotmesh-server
echo "creating container: dotmesh-builder-server"
docker rm -f dotmesh-builder-server || true
docker run \
  --name dotmesh-builder-server \
  -e GOPATH=/go \
  -w /go/src/github.com/dotmesh-io/dotmesh/cmd/dotmesh-server/pkg/main \
  dotmesh-builder \
  /usr/lib/go-1.7/bin/go build -ldflags "-X main.serverVersion=${VERSION}" -o /target/dotmesh-server
echo "copy binary: /target/dotmesh-server"
docker cp dotmesh-builder-server:/target/dotmesh-server target/
docker rm -f dotmesh-builder-server
echo "building image: ${CI_DOCKER_SERVER_IMAGE}"

docker build -t "${CI_DOCKER_SERVER_IMAGE}" .

# allow disabling of registry push
if [ -z "${NO_PUSH}" ]; then
   docker push ${CI_DOCKER_SERVER_IMAGE}
   docker push ${CI_DOCKER_PROVISIONER_IMAGE}
fi
