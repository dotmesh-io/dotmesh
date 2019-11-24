#!/bin/sh
set -xe
mkdir -p outputs

NAME=zfs-builder-$$

docker build -f Dockerfile.0-6 -t dotmesh/zfs-userland-0.6 .
docker build -f Dockerfile.0-7 -t dotmesh/zfs-userland-0.7 .
docker build -f Dockerfile.0-8 -t dotmesh/zfs-userland-0.8 .

# By manual inspection (using ldd) the following files are required

#   BIONIC

#   zfs-0.7/sbin/zfs
#   zfs-0.7/sbin/zpool
#   zfs-0.7/sbin/mount.zfs
#   zfs-0.7/lib/x86_64-linux-gnu/libblkid.so.1
#   zfs-0.7/lib/x86_64-linux-gnu/libc.so.6
#   zfs-0.7/lib/x86_64-linux-gnu/libdl.so.2
#   zfs-0.7/lib/x86_64-linux-gnu/libm.so.6
#   zfs-0.7/lib/libnvpair.so.1
#   zfs-0.7/lib/x86_64-linux-gnu/libpthread.so.0
#   zfs-0.7/lib/x86_64-linux-gnu/librt.so.1
#   zfs-0.7/lib/x86_64-linux-gnu/libuuid.so.1
#   zfs-0.7/lib/libuutil.so.1
#   zfs-0.7/lib/x86_64-linux-gnu/libz.so.1
#   zfs-0.7/lib/libzfs.so.2
#   zfs-0.7/lib/libzfs_core.so.1
#   zfs-0.7/lib/libzpool.so.2

#   ARTFUL

#   zfs-0.6/sbin/zfs
#   zfs-0.6/sbin/zpool
#   zfs-0.6/sbin/mount.zfs
#   zfs-0.6/lib/x86_64-linux-gnu/libblkid.so.1
#   zfs-0.6/lib/x86_64-linux-gnu/libc.so.6
#   zfs-0.6/lib/x86_64-linux-gnu/libm.so.6
#   zfs-0.6/lib/libnvpair.so.1
#   zfs-0.6/lib/x86_64-linux-gnu/libpthread.so.0
#   zfs-0.6/lib/x86_64-linux-gnu/librt.so.1
#   zfs-0.6/lib/x86_64-linux-gnu/libuuid.so.1
#   zfs-0.6/lib/libuutil.so.1
#   zfs-0.6/lib/x86_64-linux-gnu/libz.so.1
#   zfs-0.6/lib/libzfs.so.2
#   zfs-0.6/lib/libzfs_core.so.1
#   zfs-0.6/lib/libzpool.so.2

#   EOAN

#   zfs-0.8/lib64/ld-linux-x86-64.so.2
#   zfs-0.8/lib/libnvpair.so.1
#   zfs-0.8/lib/libuutil.so.1
#   zfs-0.8/lib/libzfs_core.so.1
#   zfs-0.8/lib/libzfs.so.2
#   zfs-0.8/lib/x86_64-linux-gnu/libblkid.so.1
#   zfs-0.8/lib/x86_64-linux-gnu/libc.so.6
#   zfs-0.8/lib/x86_64-linux-gnu/libdl.so.2
#   zfs-0.8/lib/x86_64-linux-gnu/libm.so.6
#   zfs-0.8/lib/x86_64-linux-gnu/libpthread.so.0
#   zfs-0.8/lib/x86_64-linux-gnu/libuuid.so.1
#   zfs-0.8/lib/x86_64-linux-gnu/libz.so.1
#   zfs-0.8/usr/lib/x86_64-linux-gnu/libcrypto.so.1.1

# linux-vdso.so.1 ???

docker run --rm --name $NAME -i --privileged -v ${PWD}/outputs:/outputs dotmesh/zfs-userland-0.6 bash -c '
    debootstrap artful /opt/zfs-0.6
    cd /var/cache && find . && \
    groupadd crontab && groupadd messagebus && \
    dpkg --root=/opt/zfs-0.6 -i /var/cache/apt/archives/python3-minimal*.deb
    dpkg --root=/opt/zfs-0.6 -i /var/cache/apt/archives/*.deb
    cd /opt/ && \
    tar cfh /outputs/zfs-0.6.tar zfs-0.6/sbin/zfs zfs-0.6/sbin/zpool zfs-0.6/sbin/mount.zfs zfs-0.6/lib/x86_64-linux-gnu/libblkid.so.1 zfs-0.6/lib/x86_64-linux-gnu/libc.so.6 zfs-0.6/lib/x86_64-linux-gnu/libm.so.6 zfs-0.6/lib/libnvpair.so.1 zfs-0.6/lib/x86_64-linux-gnu/libpthread.so.0 zfs-0.6/lib/x86_64-linux-gnu/librt.so.1 zfs-0.6/lib/x86_64-linux-gnu/libuuid.so.1 zfs-0.6/lib/libuutil.so.1 zfs-0.6/lib/x86_64-linux-gnu/libz.so.1 zfs-0.6/lib/libzfs.so.2 zfs-0.6/lib/libzfs_core.so.1 zfs-0.6/lib/libzpool.so.2
'

docker run --rm --name $NAME -i --privileged -v ${PWD}/outputs:/outputs dotmesh/zfs-userland-0.7 bash -c '
    debootstrap bionic /opt/zfs-0.7
    cd /var/cache && find . && \
    groupadd crontab && groupadd messagebus && \
    dpkg --root=/opt/zfs-0.7 -i /var/cache/apt/archives/libpython3*.deb
    dpkg --root=/opt/zfs-0.7 -i /var/cache/apt/archives/python3-minimal*.deb
    dpkg --root=/opt/zfs-0.7 -i /var/cache/apt/archives/*.deb
    cd /opt/ && \
    tar cfh /outputs/zfs-0.7.tar zfs-0.7/sbin/zfs zfs-0.7/sbin/zpool zfs-0.7/sbin/mount.zfs zfs-0.7/lib/x86_64-linux-gnu/libblkid.so.1 zfs-0.7/lib/x86_64-linux-gnu/libc.so.6 zfs-0.7/lib/x86_64-linux-gnu/libdl.so.2 zfs-0.7/lib/x86_64-linux-gnu/libm.so.6 zfs-0.7/lib/libnvpair.so.1 zfs-0.7/lib/x86_64-linux-gnu/libpthread.so.0 zfs-0.7/lib/x86_64-linux-gnu/librt.so.1 zfs-0.7/lib/x86_64-linux-gnu/libuuid.so.1 zfs-0.7/lib/libuutil.so.1 zfs-0.7/lib/x86_64-linux-gnu/libz.so.1 zfs-0.7/lib/libzfs.so.2 zfs-0.7/lib/libzfs_core.so.1 zfs-0.7/lib/libzpool.so.2
'

docker run --rm --name $NAME -i --privileged -v ${PWD}/outputs:/outputs dotmesh/zfs-userland-0.8 bash -c '
    debootstrap eoan /opt/zfs-0.8
    cd /var/cache && find . && \
    groupadd crontab && groupadd messagebus && \
    dpkg --root=/opt/zfs-0.8 -i /var/cache/apt/archives/libpython3*.deb
    dpkg --root=/opt/zfs-0.8 -i /var/cache/apt/archives/python3-minimal*.deb
    dpkg --root=/opt/zfs-0.8 -i /var/cache/apt/archives/*.deb
    cd /opt/ && \
    tar cfh /outputs/zfs-0.8.tar zfs-0.8/lib64/ld-linux-x86-64.so.2 zfs-0.8/lib/libnvpair.so.1 zfs-0.8/lib/libuutil.so.1 zfs-0.8/lib/libzfs_core.so.1 zfs-0.8/lib/libzfs.so.2 zfs-0.8/lib/x86_64-linux-gnu/libblkid.so.1 zfs-0.8/lib/x86_64-linux-gnu/libc.so.6 zfs-0.8/lib/x86_64-linux-gnu/libdl.so.2 zfs-0.8/lib/x86_64-linux-gnu/libm.so.6 zfs-0.8/lib/x86_64-linux-gnu/libpthread.so.0 zfs-0.8/lib/x86_64-linux-gnu/libuuid.so.1 zfs-0.8/lib/x86_64-linux-gnu/libz.so.1 zfs-0.8/usr/lib/x86_64-linux-gnu/libcrypto.so.1.1
'
