#!/bin/sh
set -xe
mkdir -p outputs/zfs-{0.6,0.7}

NAME=zfs-builder-$$

docker build -f Dockerfile.0-6 -t dotmesh/zfs-userland-0.6 .
#docker build -f Dockerfile.0-7 -t dotmesh/zfs-userland-0.7 .

docker run --name $NAME -i --privileged dotmesh/zfs-userland-0.6 bash -c '
    debootstrap artful /opt/zfs-0.6
'

docker commit $NAME dotmesh/zfs-userland-0.6-debootstrapped
docker rm -f $NAME

docker run --name $NAME -i --privileged dotmesh/zfs-userland-0.6-debootstrapped bash -c '
    cd /var/cache && find . && \
    groupadd crontab && groupadd messagebus && \
    dpkg --root=/opt/zfs-0.6 -i /var/cache/apt/archives/python3-minimal*.deb
    dpkg --root=/opt/zfs-0.6 -i /var/cache/apt/archives/*.deb
'

docker commit $NAME dotmesh/zfs-userland-0.6-installed
docker rm -f $NAME
