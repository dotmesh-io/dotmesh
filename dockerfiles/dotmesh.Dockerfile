FROM golang:1.12.5 AS build-env
ARG VERSION
ARG STABLE_DOCKER_TAG
WORKDIR /usr/local/go/src/github.com/dotmesh-io/dotmesh
RUN apk update && apk upgrade && \
    apk add --no-cache bash git openssh
COPY ./cmd /usr/local/go/src/github.com/dotmesh-io/dotmesh/cmd
COPY ./pkg /usr/local/go/src/github.com/dotmesh-io/dotmesh/pkg
COPY ./vendor /usr/local/go/src/github.com/dotmesh-io/dotmesh/vendor
RUN cd cmd/dotmesh-server && go install -ldflags="-linkmode external -extldflags -static -X main.serverVersion=${VERSION}"
RUN cd cmd/dm && go install -ldflags="-linkmode external -extldflags -static -X main.clientVersion=${VERSION} -X main.stableDockerTag=${STABLE_DOCKER_TAG}"
RUN cd cmd/flexvolume && go install -ldflags="-linkmode external -extldflags -static"

FROM ubuntu:bionic
ENV SECURITY_UPDATES 2018-08-02a
# (echo 'search ...') Merge kernel module search paths from CentOS and Ubuntu :-O
RUN apt-get -y update && apt-get -y install iproute2 kmod curl && \
    echo 'search updates extra ubuntu built-in weak-updates' > /etc/depmod.d/ubuntu.conf && \
    mkdir /tmp/d && \
    curl -o /tmp/d/docker.tgz \
        https://download.docker.com/linux/static/edge/x86_64/docker-17.10.0-ce.tgz && \
    cd /tmp/d && \
    tar zxfv /tmp/d/docker.tgz && \
    cp /tmp/d/docker/docker /usr/local/bin && \
    chmod +x /usr/local/bin/docker && \
    rm -rf /tmp/d && \
    cd /opt && curl https://get.dotmesh.io/zfs-userland/zfs-0.6.tar.gz |tar xzf - && \
    curl https://get.dotmesh.io/zfs-userland/zfs-0.7.tar.gz |tar xzf -
COPY ./cmd/dotmesh-server/require_zfs.sh /require_zfs.sh
COPY --from=build-env /usr/local/go/bin/flexvolume /usr/local/bin/
COPY --from=build-env /usr/local/go/bin/dotmesh-server /usr/local/bin/
COPY --from=build-env /usr/local/go/bin/dm /usr/local/bin/
COPY ./scripts/dm-remote-add /usr/local/bin/