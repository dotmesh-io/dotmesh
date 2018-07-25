FROM ubuntu:artful
ENV SECURITY_UPDATES 2018-01-19
RUN apt-get -y update && apt-get -y install zfsutils-linux iproute kmod curl
# Merge kernel module search paths from CentOS and Ubuntu :-O
RUN echo 'search updates extra ubuntu built-in weak-updates' > /etc/depmod.d/ubuntu.conf
RUN mkdir /tmp/d && \
    curl -o /tmp/d/docker.tgz \
        https://download.docker.com/linux/static/edge/x86_64/docker-17.10.0-ce.tgz && \
    cd /tmp/d && \
    tar zxfv /tmp/d/docker.tgz && \
    cp /tmp/d/docker/docker /usr/local/bin && \
    chmod +x /usr/local/bin/docker && \
    rm -rf /tmp/d
ADD require_zfs.sh /require_zfs.sh
