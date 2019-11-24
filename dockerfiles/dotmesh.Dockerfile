FROM golang:1.12.5 AS build-env
ARG VERSION
ARG STABLE_DOCKER_TAG
WORKDIR /usr/local/go/src/github.com/dotmesh-io/dotmesh
COPY ./cmd /usr/local/go/src/github.com/dotmesh-io/dotmesh/cmd
COPY ./pkg /usr/local/go/src/github.com/dotmesh-io/dotmesh/pkg
COPY ./vendor /usr/local/go/src/github.com/dotmesh-io/dotmesh/vendor
RUN cd cmd/dotmesh-server && go install -ldflags="-X main.serverVersion=${VERSION}"
RUN cd cmd/dm && go install -ldflags="-s -X main.clientVersion=${VERSION} -X main.stableDockerTag=${STABLE_DOCKER_TAG}"
RUN cd cmd/flexvolume && go install

FROM ubuntu:eoan
ENV SECURITY_UPDATES 2019-11-24a
# (echo 'search ...') Merge kernel module search paths from CentOS and Ubuntu :-O
RUN apt-get -y update && apt-get -y install iproute2 kmod curl && \
    echo 'search updates extra ubuntu built-in weak-updates' > /etc/depmod.d/ubuntu.conf && \
    mkdir /tmp/d && \
    curl -o /tmp/d/docker.tgz \
        https://download.docker.com/linux/static/stable/x86_64/docker-19.03.5.tgz && \
    cd /tmp/d && \
    tar zxfv /tmp/d/docker.tgz && \
    cp /tmp/d/docker/docker /usr/local/bin && \
    chmod +x /usr/local/bin/docker && \
    rm -rf /tmp/d && \
    cd /opt && curl https://get.dotmesh.io/zfs-userland/zfs-0.6.tar.gz |tar xzf - && \
    curl https://get.dotmesh.io/zfs-userland/zfs-0.7.tar.gz |tar xzf - && \
    curl https://get.dotmesh.io/zfs-userland/zfs-0.8.tar.gz |tar xzf -
COPY ./cmd/dotmesh-server/require_zfs.sh /require_zfs.sh
COPY --from=build-env /usr/local/go/bin/flexvolume /usr/local/bin/
COPY --from=build-env /usr/local/go/bin/dotmesh-server /usr/local/bin/
COPY --from=build-env /usr/local/go/bin/dm /usr/local/bin/
COPY ./scripts/dm-remote-add /usr/local/bin/
