#!/usr/bin/env bash

# NOTE - I have split these apart so there is an option to rebuild only the
# dotmesh server without building the k8s binaries
# this yields a big time saving when only developing the frontend stack
# without k8s - export DISABLE_K8S=1 disables the k8s build
set -xe

CI_DOCKER_SERVER_IMAGE=${CI_DOCKER_SERVER_IMAGE:=$(hostname).local:80/dotmesh/dotmesh-server:latest}
CI_DOCKER_PROVISIONER_IMAGE=${CI_DOCKER_PROVISIONER_IMAGE:=$(hostname).local:80/dotmesh/dotmesh-dynamic-provisioner:latest}
VERSION="$(cd ../versioner && go run versioner.go)"

if [ x$CI_DOCKER_TAG == x ]
then
         # Non-CI build
         CI_DOCKER_TAG=latest
fi

mkdir -p target

echo "building image: dotmesh-builder:$CI_DOCKER_TAG"
docker build --build-arg VERSION="${VERSION}" -f Dockerfile.build -t dotmesh-builder:$CI_DOCKER_TAG .

# docker
echo "creating container: dotmesh-builder-docker-$CI_DOCKER_TAG"
docker rm -f dotmesh-builder-docker-$CI_DOCKER_TAG || true
docker create \
  --name dotmesh-builder-docker-$CI_DOCKER_TAG \
  dotmesh-builder:$CI_DOCKER_TAG
echo "copy binary: /target/docker"
docker cp dotmesh-builder-docker-$CI_DOCKER_TAG:/target/docker target/
docker rm -f dotmesh-builder-docker-$CI_DOCKER_TAG

# skip rebuilding Kubernetes components if not using them
if [ -z "${SKIP_K8S}" ]; then

  # flexvolume
  echo "creating container: dotmesh-builder-flexvolume-$CI_DOCKER_TAG"
  docker rm -f dotmesh-builder-flexvolume-$CI_DOCKER_TAG || true
  docker run \
    --name dotmesh-builder-flexvolume-$CI_DOCKER_TAG \
    -e GOPATH=/go \
    -w /go/src/github.com/dotmesh-io/dotmesh/cmd/dotmesh-server/pkg/flexvolume \
    dotmesh-builder:$CI_DOCKER_TAG \
    /usr/lib/go-1.7/bin/go build -o /target/flexvolume
  echo "copy binary: /target/flexvolume"
  docker cp dotmesh-builder-flexvolume-$CI_DOCKER_TAG:/target/flexvolume target/
  docker rm -f dotmesh-builder-flexvolume-$CI_DOCKER_TAG

  # dm-provisioner
  echo "creating container: dotmesh-builder-dm-provisioner-$CI_DOCKER_TAG"
  docker rm -f dotmesh-builder-dm-provisioner-$CI_DOCKER_TAG || true
  docker run \
    --name dotmesh-builder-dm-provisioner-$CI_DOCKER_TAG \
    -e GOPATH=/go \
    -e CGO_ENABLED=0 \
    -w /go/src/github.com/dotmesh-io/dotmesh/cmd/dotmesh-server/pkg/dynamic-provisioning \
    dotmesh-builder:$CI_DOCKER_TAG \
    /usr/lib/go-1.7/bin/go build -a -ldflags '-extldflags "-static"' -o /target/dm-provisioner .
  echo "copy binary: /target/dm-provisioner"
  docker cp dotmesh-builder-dm-provisioner-$CI_DOCKER_TAG:/target/dm-provisioner target/
  docker rm -f dotmesh-builder-dm-provisioner-$CI_DOCKER_TAG

  echo "building image: ${CI_DOCKER_PROVISIONER_IMAGE}"
  docker build -f pkg/dynamic-provisioning/Dockerfile -t "${CI_DOCKER_PROVISIONER_IMAGE}" .
fi

# dotmesh-server
echo "creating container: dotmesh-builder-server-$CI_DOCKER_TAG"
docker rm -f dotmesh-builder-server-$CI_DOCKER_TAG || true
docker run \
  --name dotmesh-builder-server-$CI_DOCKER_TAG \
  -e GOPATH=/go \
  -w /go/src/github.com/dotmesh-io/dotmesh/cmd/dotmesh-server/pkg/main \
  dotmesh-builder:$CI_DOCKER_TAG \
  /usr/lib/go-1.7/bin/go build -ldflags "-X main.serverVersion=${VERSION}" -o /target/dotmesh-server
echo "copy binary: /target/dotmesh-server"
docker cp dotmesh-builder-server-$CI_DOCKER_TAG:/target/dotmesh-server target/
docker rm -f dotmesh-builder-server-$CI_DOCKER_TAG
echo "building image: ${CI_DOCKER_SERVER_IMAGE}"

docker build -t "${CI_DOCKER_SERVER_IMAGE}" .

# allow disabling of registry push
if [ -z "${NO_PUSH}" ]; then
   docker push ${CI_DOCKER_SERVER_IMAGE}
   docker push ${CI_DOCKER_PROVISIONER_IMAGE}
fi
