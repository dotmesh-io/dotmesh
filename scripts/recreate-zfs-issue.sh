#!/bin/sh

set -ex

ROOT=/tmp/test.$$
POOL=test-$$
IMAGE=nixos.local:80/dotmesh/dotmesh-server

##### SETUP

mkdir -p $ROOT
mkdir -p $ROOT/m0
mkdir -p $ROOT/m1

truncate -s 10G $ROOT/pool0
truncate -s 10G $ROOT/pool1

zpool create -m $ROOT/m0 $POOL-node-0 $ROOT/pool0
zpool create -m $ROOT/m1 $POOL-node-1 $ROOT/pool1

zfs create -o mountpoint=legacy $POOL-node-0/dmfs
zfs create -o mountpoint=legacy $POOL-node-1/dmfs

mkdir $ROOT/m0/dmfs
mkdir $ROOT/m1/dmfs

C0=`docker run -d --privileged --pid=host -v $ROOT:$ROOT:rshared $IMAGE /bin/sh -c 'mknod -m 660 /dev/zfs c $(cat /sys/class/misc/zfs/dev |sed "s/:/ /g") ; mount -t proc proc /proc ; sleep 1000000'`
C1=`docker run -d --privileged --pid=host -v $ROOT:$ROOT:rshared $IMAGE /bin/sh -c 'mknod -m 660 /dev/zfs c $(cat /sys/class/misc/zfs/dev |sed "s/:/ /g") ; mount -t proc proc /proc ; sleep 1000000'`

sleep 30

run_n0 () {
    docker exec -i $C0 "$@"
}

run_n1 () {
    docker exec -i $C1 "$@"
}

run_client () {
    FS=$1
    shift
    docker run -v $FS:/foo busybox "$@"
}

#### NODE 0

run_n0 zfs create $POOL-node-0/dmfs/f0af07d9-10bb-4227-5443-f172ceff6db4 # f0af07d9-10bb-4227-5443-f172ceff6db4
run_n0 mkdir $ROOT/m0/dmfs/f0af07d9-10bb-4227-5443-f172ceff6db4
run_n0 mount.zfs -o noatime $POOL-node-0/dmfs/f0af07d9-10bb-4227-5443-f172ceff6db4 $ROOT/m0/dmfs/f0af07d9-10bb-4227-5443-f172ceff6db4 # f0af07d9-10bb-4227-5443-f172ceff6db4
run_client $ROOT/m0/dmfs/f0af07d9-10bb-4227-5443-f172ceff6db4 touch /foo/HELLO
run_n0 zfs snapshot -o io.dotmesh:meta-author=YWRtaW4= -o io.dotmesh:meta-message=Rmlyc3QgY29tbWl0 -o io.dotmesh:meta-timestamp=MTUzMTg0MjYzNTE2MDc0OTk2NQ== $POOL-node-0/dmfs/f0af07d9-10bb-4227-5443-f172ceff6db4@2f1ef1de-d6a8-4663-5ee7-83e20181ff84 # f0af07d9-10bb-4227-5443-f172ceff6db4
run_n0 zfs send -p -R $POOL-node-0/dmfs/f0af07d9-10bb-4227-5443-f172ceff6db4@2f1ef1de-d6a8-4663-5ee7-83e20181ff84 > $ROOT/zfs-send-1 # f0af07d9-10bb-4227-5443-f172ceff6db4

#### NODE 1
run_n1 zfs recv $POOL-node-1/dmfs/f0af07d9-10bb-4227-5443-f172ceff6db4 < $ROOT/zfs-send-1 # f0af07d9-10bb-4227-5443-f172ceff6db4
run_n1 mkdir $ROOT/m1/dmfs/f0af07d9-10bb-4227-5443-f172ceff6db4
run_n1 mount.zfs -o noatime $POOL-node-1/dmfs/f0af07d9-10bb-4227-5443-f172ceff6db4 $ROOT/m1/dmfs/f0af07d9-10bb-4227-5443-f172ceff6db4 # f0af07d9-10bb-4227-5443-f172ceff6db4
run_client $ROOT/m1/dmfs/f0af07d9-10bb-4227-5443-f172ceff6db4 touch /foo/HELLO2
run_n1 zfs snapshot -o io.dotmesh:meta-timestamp=MTUzMTg0MjY1MDc3Mjg2NDQyOA== -o io.dotmesh:meta-author=YWRtaW4= -o io.dotmesh:meta-message=bm9kZTIgY29tbWl0 $POOL-node-1/dmfs/f0af07d9-10bb-4227-5443-f172ceff6db4@eafb2e4c-fc06-4220-6f68-b21a54773d22 # f0af07d9-10bb-4227-5443-f172ceff6db4

#### NODE 0
run_n0 umount $ROOT/m0/dmfs/f0af07d9-10bb-4227-5443-f172ceff6db4 # f0af07d9-10bb-4227-5443-f172ceff6db4
run_n0 mount.zfs -o noatime $POOL-node-0/dmfs/f0af07d9-10bb-4227-5443-f172ceff6db4 $ROOT/m0/dmfs/f0af07d9-10bb-4227-5443-f172ceff6db4 # f0af07d9-10bb-4227-5443-f172ceff6db4
umount $ROOT/m0/dmfs/f0af07d9-10bb-4227-5443-f172ceff6db4 # f0af07d9-10bb-4227-5443-f172ceff6db4
run_n0 mount.zfs -o noatime $POOL-node-0/dmfs/f0af07d9-10bb-4227-5443-f172ceff6db4 $ROOT/m0/dmfs/f0af07d9-10bb-4227-5443-f172ceff6db4 # f0af07d9-10bb-4227-5443-f172ceff6db4???
zfs snapshot $POOL-node-0/dmfs/f0af07d9-10bb-4227-5443-f172ceff6db4@Node1CommitHash
run_n0 zfs send -p -I 2f1ef1de-d6a8-4663-5ee7-83e20181ff84 $POOL-node-0/dmfs/f0af07d9-10bb-4227-5443-f172ceff6db4@Node1CommitHash > $ROOT/zfs-send-2 # f0af07d9-10bb-4227-5443-f172ceff6db4

### zfs recv $POOL-node-0/dmfs/f93c239e-13ce-4b33-5a99-ec7f9e134e28 # f93c239e-13ce-4b33-5a99-ec7f9e134e28

#### NODE 1

run_n1 umount $ROOT/m1/dmfs/f0af07d9-10bb-4227-5443-f172ceff6db4 # f0af07d9-10bb-4227-5443-f172ceff6db4
run_n1 zfs rename $POOL-node-1/dmfs/f0af07d9-10bb-4227-5443-f172ceff6db4 $POOL-node-1/dmfs/f93c239e-13ce-4b33-5a99-ec7f9e134e28 # f0af07d9-10bb-4227-5443-f172ceff6db4
run_n1 zfs clone $POOL-node-1/dmfs/f93c239e-13ce-4b33-5a99-ec7f9e134e28@2f1ef1de-d6a8-4663-5ee7-83e20181ff84 $POOL-node-1/dmfs/f0af07d9-10bb-4227-5443-f172ceff6db4 # f0af07d9-10bb-4227-5443-f172ceff6db4
run_n1 zfs promote $POOL-node-1/dmfs/f0af07d9-10bb-4227-5443-f172ceff6db4 # f0af07d9-10bb-4227-5443-f172ceff6db4
run_n1 mkdir $ROOT/m1/dmfs/f93c239e-13ce-4b33-5a99-ec7f9e134e28
run_n1 mount.zfs -o noatime $POOL-node-1/dmfs/f93c239e-13ce-4b33-5a99-ec7f9e134e28 $ROOT/m1/dmfs/f93c239e-13ce-4b33-5a99-ec7f9e134e28 # f93c239e-13ce-4b33-5a99-ec7f9e134e28

run_n1 zfs recv $POOL-node-1/dmfs/f0af07d9-10bb-4227-5443-f172ceff6db4 < $ROOT/zfs-send-2 # f0af07d9-10bb-4227-5443-f172ceff6db4

run_n1 zfs send -p -I $POOL-node-1/dmfs/f0af07d9-10bb-4227-5443-f172ceff6db4@2f1ef1de-d6a8-4663-5ee7-83e20181ff84 $POOL-node-1/dmfs/f93c239e-13ce-4b33-5a99-ec7f9e134e28@eafb2e4c-fc06-4220-6f68-b21a54773d22 > $ROOT/zfs-send-3 # f93c239e-13ce-4b33-5a99-ec7f9e134e28

#### Test mounts

run_n1 mount.zfs -o noatime $POOL-node-1/dmfs/f0af07d9-10bb-4227-5443-f172ceff6db4 $ROOT/m1/dmfs/f0af07d9-10bb-4227-5443-f172ceff6db4 # f0af07d9-10bb-4227-5443-f172ceff6db4

echo "Mount in $ROOT/m1/dmfs/f0af07d9-10bb-4227-5443-f172ceff6db4"
