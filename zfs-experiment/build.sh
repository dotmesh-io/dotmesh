#!/bin/sh
set -xe
mkdir -p outputs

NAME=zfs-builder-$$

docker build -f Dockerfile.0-6 -t dotmesh/zfs-userland-0.6 .
docker build -f Dockerfile.0-7 -t dotmesh/zfs-userland-0.7 .

docker run --rm --name $NAME -i --privileged -v ${PWD}/outputs:/outputs dotmesh/zfs-userland-0.6 bash -c '
    debootstrap artful /opt/zfs-0.6
    cd /var/cache && find . && \
    groupadd crontab && groupadd messagebus && \
    dpkg --root=/opt/zfs-0.6 -i /var/cache/apt/archives/python3-minimal*.deb
    dpkg --root=/opt/zfs-0.6 -i /var/cache/apt/archives/*.deb
    cd /opt/ && \
    tar cf /outputs/zfs-0.6.tar zfs-0.6
'

docker run --rm --name $NAME -i --privileged -v ${PWD}/outputs:/outputs dotmesh/zfs-userland-0.7 bash -c '
    debootstrap bionic /opt/zfs-0.7
    cd /var/cache && find . && \
    groupadd crontab && groupadd messagebus && \
    dpkg --root=/opt/zfs-0.7 -i /var/cache/apt/archives/libpython3*.deb
    dpkg --root=/opt/zfs-0.7 -i /var/cache/apt/archives/python3-minimal*.deb
    dpkg --root=/opt/zfs-0.7 -i /var/cache/apt/archives/*.deb
    cd /opt/ && \
    tar cf /outputs/zfs-0.7.tar zfs-0.7
'
