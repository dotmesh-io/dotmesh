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
ADD require_zfs.sh /require_zfs.sh
