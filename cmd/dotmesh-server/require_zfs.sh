#!/bin/bash
set -xe

function fetch_zfs {
    KERN=$(uname -r)
    RELEASE=zfs-${KERN}.tar.gz
    cd /bundled-lib
    if [ -d /bundled-lib/lib/modules ]; then
        # Try loading a cached module (which we cached in a docker
        # volume)
        depmod -b /bundled-lib || true
        if modprobe -d /bundled-lib zfs; then
            echo "Successfully loaded cached ZFS for $KERN :)"
            return
        else
            echo "Unable to load cached module, trying to fetch one (maybe you upgraded your kernel)..."
            mv /bundled-lib/lib /bundled-lib/lib.backup-`date +%s`
        fi
    fi
    if ! curl -f -o ${RELEASE} https://get.dotmesh.io/zfs/${RELEASE}; then
        echo "ZFS is not installed on your docker host, and unable to find a kernel module for your kernel: $KERN"
        echo "Please create a new GitHub issue, pasting this error message, and tell me which Linux distribution you are using, at:"
        echo
        echo "    https://github.com/dotmesh-io/dotmesh/issues"
        echo
        echo "Meanwhile, you should still be able to use dotmesh if you install ZFS manually on your host system by following the instructions at http://zfsonlinux.org/ and then re-run the dotmesh installer."
        echo
        echo "Alternatively, Ubuntu 16.04 and later comes with ZFS preinstalled, so using that should Just Work. Kernel modules for Docker for Mac and other Docker distributions are also provided."
        exit 1
    fi
    tar xf ${RELEASE}
    depmod -b /bundled-lib || true
    modprobe -d /bundled-lib zfs
    echo "Successfully loaded downloaded ZFS for $KERN :)"
}

# Find the hostname from the actual host, rather than the container.
HOSTNAME="`nsenter -t 1 -m -u -n -i hostname || echo unknown`"

# Put the data file inside /var/lib so that we end up on the big
# partition if we're in a LinuxKit env.
POOL_SIZE=${POOL_SIZE:-10G}
DIR=${USE_POOL_DIR:-/var/lib/dotmesh}
DIR=$(echo $DIR |sed s/\#HOSTNAME\#/$HOSTNAME/)
FILE=${DIR}/dotmesh_data
POOL=${USE_POOL_NAME:-pool}
POOL=$(echo $POOL |sed s/\#HOSTNAME\#/$HOSTNAME/)
DOTMESH_INNER_SERVER_NAME=${DOTMESH_INNER_SERVER_NAME:-dotmesh-server-inner}
FLEXVOLUME_DRIVER_DIR=${FLEXVOLUME_DRIVER_DIR:-/usr/libexec/kubernetes/kubelet-plugins/volume/exec}
INHERIT_ENVIRONMENT_NAMES=( "DOTMESH_SERVER_PORT" "FILESYSTEM_METADATA_TIMEOUT" "DOTMESH_UPGRADES_URL" "DOTMESH_UPGRADES_INTERVAL_SECONDS" "NATS_URL" "NATS_USERNAME" "NATS_PASSWORD" "NATS_SUBJECT_PREFIX" "DOTMESH_STORAGE" "DOTMESH_BOLTDB_PATH" "EXTERNAL_USER_MANAGER_URL" "DISABLE_DIRTY_POLLING" "POLL_DIRTY_SUCCESS_TIMEOUT" "POLL_DIRTY_ERROR_TIMEOUT")

if [ $POOL_SIZE = AUTO ]
then
    # Scale the pool to the size of the filesystem, minus 16MiB for metadata files
    MIB_FREE=`df --sync -B 1048576 --output=avail "$DIR" | tail -1`
    POOL_SIZE="$(( $MIB_FREE - 16 ))M"
    echo "Automatic pool size selection: $MIB_FREE megabytes free in $DIR, setting pool size to $POOL_SIZE"
fi

echo "=== Using storage dir $DIR and mountpoint $MOUNTPOINT"

# Docker volume where we can cache downloaded, "bundled" zfs
BUNDLED_LIB=/bundled-lib
# Bind-mounted system library where we can attempt to modprobe any
# system-provided zfs modules (e.g. Ubuntu 16.04) or those manually installed
# by user
SYSTEM_LIB=/system-lib

# If we're asked to find the outer mount, then we need to feed `zpool` the
# path to where it's mounted _on the host_. So, nsenter up to the host and
# find where the block device is mounted there.
#
# This assumes that $DIR _is_ the mountpoint of the block device, for
# example a kubernetes-provided PV.
#
# Further reading: https://github.com/dotmesh-io/dotmesh/issues/333

if [ -n "$CONTAINER_POOL_MNT" ]; then
    echo "Attaching to a zpool from a container mount. $DIR is the mountpoint in the container..."
    BLOCK_DEVICE=`mount | grep $DIR | cut -d ' ' -f 1 | head -n 1`
    echo "$DIR seems to be mounted from $BLOCK_DEVICE"
    OUTER_DIR=`nsenter -t 1 -m -u -n -i /bin/sh -c 'mount' | grep $BLOCK_DEVICE | cut -f 3 -d ' ' | head -n 1`
    echo "$BLOCK_DEVICE seems to be mounted on $OUTER_DIR in the host"
fi

if [ -n "$CONTAINER_POOL_PVC_NAME" ]; then
    WORK_ROOT=/var/lib/dotmesh-mounts/$CONTAINER_POOL_PVC_NAME
    EXTRA_VOLUMES="-v $WORK_ROOT:$WORK_ROOT:rshared"
    MOUNTPOINT=${WORK_ROOT}/mnt
    CONTAINER_MOUNT_PREFIX=${WORK_ROOT}/container_mnt

    if [ -n "$EXTRA_HOST_COMMANDS"]; then
        nsenter -t 1 -m -u -n -i /bin/sh -c "$EXTRA_HOST_COMMANDS"
    fi
    nsenter -t 1 -m -u -n -i /bin/sh -c "mkdir -p $WORK_ROOT"

    echo "Using $WORK_ROOT as a mount workspace."
else
    BLOCK_DEVICE="n/a"
    OUTER_DIR="$DIR"
    EXTRA_VOLUMES=""

    # Set up paths we'll use for stuff
    MOUNTPOINT=${MOUNTPOINT:-$OUTER_DIR/mnt}
    CONTAINER_MOUNT_PREFIX=${CONTAINER_MOUNT_PREFIX:-$OUTER_DIR/container_mnt}

    echo "Using $OUTER_DIR as a mount workspace."
fi

# Set the shared flag on the working directory on the host. This is
# essential; it, combined with the presence of the shared flag on the
# bind-mount of this into the container namespace when we run the
# dotmesh server container, means that mounts created by the dotmesh
# server will be propogated back up to the host.

# The bind mount of the outer dir onto itself is a trick to make sure
# it's actually a mount - if it's just a directory on the host (eg,
# /var/lib/dotmesh), then we can't set the shared flag on it as it's
# part of some larger mount. Creating a bind mount means we have a
# distinct mount point at the local. However, it's not necessary if it
# really IS a mountpoint already (eg, a k8s pv).

# Confused? Here's some further reading, in order:

# https://lwn.net/Articles/689856/
# https://lwn.net/Articles/690679/
# https://www.kernel.org/doc/Documentation/filesystems/sharedsubtree.txt

nsenter -t 1 -m -u -n -i /bin/sh -c \
    "set -xe
    $EXTRA_HOST_COMMANDS
    if [ $(mount |grep $OUTER_DIR |wc -l) -eq 0 ]; then
        echo \"Creating and bind-mounting shared $OUTER_DIR\"
        mkdir -p $OUTER_DIR && \
        mount --make-rshared / && \
        mount --bind $OUTER_DIR $OUTER_DIR && \
        mount --make-rshared $OUTER_DIR;
    fi
    mkdir -p /run/docker/plugins
    mkdir -p $MOUNTPOINT
    mkdir -p $CONTAINER_MOUNT_PREFIX"

if [ ! -e /sys ]; then
    mount -t sysfs sys sys/
fi

if [ ! -d $DIR ]; then
    mkdir -p $DIR
fi

# KERNEL_ZFS_VERSION may already be set from outside, by
# configuration; if so, we can skip all the attempts to load modules
# and find the version, as the user is asserting they've handled all
# of that.

if [ -z "$KERNEL_ZFS_VERSION" ]; then
    if [ -n "`lsmod|grep zfs`" ]; then
        echo "ZFS already loaded :)"
    else
        depmod -b /system-lib || true
        if ! modprobe -d /system-lib zfs; then
            fetch_zfs
        else
            echo "Successfully loaded system ZFS :)"
        fi
    fi

    # System takes precedence over bundled version - if there is a
    # system version, we'll use it
    KERNEL_ZFS_VERSION=$(modinfo -F version -b /system-lib zfs ||true)
    if [ -z "$KERNEL_ZFS_VERSION" ]; then
        KERNEL_ZFS_VERSION=$(modinfo -F version -b /bundled-lib zfs ||true)
        echo "Using bundled ZFS kernel version $KERNEL_ZFS_VERSION"
    else
        echo "Using system ZFS kernel version $KERNEL_ZFS_VERSION"
    fi
fi

if [[ "$KERNEL_ZFS_VERSION" == "0.6"* ]]; then
    echo "Detected ZFS 0.6 kernel modules ($KERNEL_ZFS_VERSION), using matching userland"
    export ZFS_USERLAND_ROOT=/opt/zfs-0.6
elif [[ "$KERNEL_ZFS_VERSION" == "0.7"* ]]; then
    echo "Detected ZFS 0.7 kernel modules ($KERNEL_ZFS_VERSION), using matching userland"
    export ZFS_USERLAND_ROOT=/opt/zfs-0.7
elif [[ "$KERNEL_ZFS_VERSION" == "0.8"* ]]; then
    echo "Detected ZFS 0.8 kernel modules ($KERNEL_ZFS_VERSION), using matching userland"
    export ZFS_USERLAND_ROOT=/opt/zfs-0.8
else
    echo "Kernel ZFS version ($KERNEL_ZFS_VERSION) doesn't match 0.6, 0.7 or 0.8, not supported"
    echo
    echo "Trying to download kernel modules again and then restarting in case we're hitting"
    echo "https://github.com/dotmesh-io/dotmesh/issues/542"
    fetch_zfs
    exit 1
fi

POOL_LOGFILE=$DIR/dotmesh_pool_log

run_in_zfs_container() {
    NAME=$1
    shift
    docker run -i --rm --pid=host --privileged --name=dotmesh-$NAME-$$ \
           -e LD_LIBRARY_PATH=$ZFS_USERLAND_ROOT/lib \
           -e PATH=$ZFS_USERLAND_ROOT/sbin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin \
           -v $OUTER_DIR:$OUTER_DIR:rshared \
           $DOTMESH_DOCKER_IMAGE \
           "$@"
}

set -ex

if [ ! -e /dev/zfs ]; then
    mknod -m 660 /dev/zfs c $(cat /sys/class/misc/zfs/dev |sed 's/:/ /g')
fi

echo "`date`: On host '$HOSTNAME', working directory = '$OUTER_DIR', device = '$BLOCK_DEVICE', zfs mountpoint = '$MOUNTPOINT', pool = '$POOL', Dotmesh image = '$DOTMESH_DOCKER_IMAGE'"

if ! run_in_zfs_container zpool-status zpool status $POOL; then

    # TODO: make case where truncate previously succeeded but zpool create
    # failed or never run recoverable.
    if [ ! -f $FILE ]; then
        truncate -s $POOL_SIZE $FILE
        run_in_zfs_container zpool-create zpool create -m none $POOL "$OUTER_DIR/dotmesh_data"
        echo "This directory contains dotmesh data files, please leave them alone unless you know what you're doing. See github.com/dotmesh-io/dotmesh for more information." > $DIR/README
        run_in_zfs_container zpool-get zpool get -H guid $POOL |cut -f 3 > $DIR/dotmesh_pool_id
        if [ -n "$CONTAINER_POOL_PVC_NAME" ]; then
            echo "$CONTAINER_POOL_PVC_NAME" > $DIR/dotmesh_pvc_name
        fi
        echo "`date`: Pool created" >> $POOL_LOGFILE
    else
        run_in_zfs_container zpool-import zpool import -f -d $OUTER_DIR $POOL
        echo "`date`: Pool imported" >> $POOL_LOGFILE
    fi
else
    echo "`date`: Pool already exists" >> $POOL_LOGFILE
fi

# Clear away stale socket if existing
rm -f /run/docker/plugins/dm.sock

# At this point, if we try and run any 'docker' commands and there are any
# dotmesh containers already on the host, we'll deadlock because docker will
# go looking for the dm plugin. So, we need to start up a fake dm plugin which
# just responds immediately with errors to everything. It will create a socket
# file which will hopefully get clobbered by the real thing.

# TODO XXX find out why the '###' commented-out bits below have to be disabled
# when running the Kubernetes tests... maybe the daemonset restarts us multiple
# times? maybe there's some leakage between dind hosts??

###dotmesh-server --temporary-error-plugin &

# Attempt to avoid the race between `temporary-error-plugin` and the real
# dotmesh-server. If `--temporary-error-plugin` loses the race, the
# plugin is broken forever.
###while [ ! -e /run/docker/plugins/dm.sock ]; do
###    echo "Waiting for /run/docker/plugins/dm.sock to exist due to temporary-error-plugin..."
###    sleep 0.1
###done

# Clear away old running server if running
docker rm -f dotmesh-server-inner || true

echo "Starting the 'real' dotmesh-server in a sub-container. Go check 'docker logs dotmesh-server-inner' if you're looking for dotmesh logs."

log_opts=""
rm_opt=""
if [ "$LOG_ADDR" != "" ]; then
    log_opts="--log-driver=syslog --log-opt syslog-address=tcp://$LOG_ADDR:5000"
#    rm_opt="--rm"
fi

# To have its port exposed on Docker for Mac, `docker run` needs -p 32607.  But
# dotmesh-server also wants to discover its routeable IPv4 addresses (on Linux
# anyway; multi-node clusters work only on Linux because we can't discover the
# Mac's IP from a container).  So to work with both we do that in the host
# network namespace (via docker) and pass it in.
YOUR_IPV4_ADDRS="$(docker run --rm -i --net=host $DOTMESH_DOCKER_IMAGE dotmesh-server --guess-ipv4-addresses)"

pki_volume_mount=""
if [ "$PKI_PATH" != "" ]; then
    pki_volume_mount="-v $PKI_PATH:/pki"
fi

PORT=${DOTMESH_SERVER_PORT:-32607}
net="-p ${PORT}:${PORT} -p 32608:32608 -p 32609:32609 -p 32610:32610 -p 32611:32611"
if [ ! -z ${DISABLE_EXPOSED_PORTS+x} ]; then
    net=""
fi
link=""

# this setting means we have set DOTMESH_ETCD_ENDPOINT to a known working
# endpoint and we don't want any links for --net flags passed to Docker
if [ -z "$DOTMESH_MANUAL_NETWORKING" ]; then
    if [ "$DOTMESH_ETCD_ENDPOINT" == "https://dotmesh-etcd:42379" ]; then
        # If etcd endpoint is overridden, then don't try to link to a local
        # dotmesh-etcd container (etcd probably is being provided externally, e.g.
        # by etcd operator on Kubernetes).
        link="--link dotmesh-etcd:dotmesh-etcd"
    fi
    if [ "$DOTMESH_JOIN_OUTER_NETWORK" == "true" ]; then
        # When running in a pod network, calculate the id of the current container
        # in scope, and pass that as --net=container:<id> so that dotmesh-server
        # itself runs in the same network namespace.
        self_containers=$(docker ps -q --filter="ancestor=$DOTMESH_DOCKER_IMAGE")
        array_containers=( $self_containers )
        num_containers=${#array_containers[@]}
        if [ $num_containers -eq 0 ]; then
            echo "Cannot find id of own container!"
            exit 1
        fi
        if [ $num_containers -gt 1 ]; then
            echo "Found more than one id of own container! $self_containers"
            exit 1
        fi
        net="--net=container:$self_containers"
        # When running in a pod network, assuming we're not on a cluster that
        # supports hostPort networking, it's preferable for the nodes to report
        # their pod IPs to eachother, rather than the external IPs calculated above
        # in `--guess-ipv4-addresses`. So, unset this environment variable so that
        # dotmesh-server has to calculate the IP from inside the container in the
        # Kubernetes pod.
        unset YOUR_IPV4_ADDRS
    fi
fi

if [ -n "$DOTMESH_JOIN_DOCKER_NETWORK" ]; then
    net="$net --net=$DOTMESH_JOIN_DOCKER_NETWORK"
fi

secret=""

# if both env vars are set then just use them
if [ -n "$INITIAL_ADMIN_PASSWORD" ] && [ -n "$INITIAL_ADMIN_API_KEY" ]; then
    export INITIAL_ADMIN_PASSWORD=$(echo -n "$INITIAL_ADMIN_PASSWORD" | base64)
    export INITIAL_ADMIN_API_KEY=$(echo -n "$INITIAL_ADMIN_API_KEY" | base64)
    secret="-e INITIAL_ADMIN_PASSWORD=$INITIAL_ADMIN_PASSWORD -e INITIAL_ADMIN_API_KEY=$INITIAL_ADMIN_API_KEY"
# otherwise we gonna use the filepaths given
else
    if [[ "$INITIAL_ADMIN_PASSWORD_FILE" != "" && \
          -e $INITIAL_ADMIN_PASSWORD_FILE && \
          "$INITIAL_ADMIN_API_KEY_FILE" != "" && \
          -e $INITIAL_ADMIN_API_KEY_FILE ]]; then
        pw=$(cat $INITIAL_ADMIN_PASSWORD_FILE |tr -d '\n' |base64 -w 0)
        ak=$(cat $INITIAL_ADMIN_API_KEY_FILE |tr -d '\n' |base64 -w 0)
        secret="-e INITIAL_ADMIN_PASSWORD=$pw -e INITIAL_ADMIN_API_KEY=$ak"
        echo "set secret: $secret"
    fi
fi

INHERIT_ENVIRONMENT_ARGS=""

for name in "${INHERIT_ENVIRONMENT_NAMES[@]}"
do
    INHERIT_ENVIRONMENT_ARGS="$INHERIT_ENVIRONMENT_ARGS -e $name=$(eval "echo \$$name")"
done

# we need the logs from the inner server to be sent to the outer container
# such that k8s will pick them up from the pod - the inner container is not
# a pod but a container run from /var/run/docker.sock
(while true; do docker logs -f dotmesh-server-inner || true; sleep 1; done) &

set +e

# In order of the -v options below:

# 1. Mount the docker socket so that we can stop and start containers around
#    e.g. dm reset.

# 2. Be able to install the docker plugin.

# 3. Be able to mount zfs filesystems from inside the container in
#    such a way that they propogate up to the host, and be able to
#    create some symlinks that we hand to the docker volume plugin.

# 4. Be able to install a Kubernetes FlexVolume driver (we make symlinks
#    where it tells us to).

# NOTE: The *source* path in the -v flags IS AS SEEN BY THE HOST,
# *not* as seen by this container running require_zfs.sh, because
# we're talking to the host docker. That's why we must map
# $OUTER_DIR:$OUTER_DIR and not $DIR:$OUTER_DIR...

docker run -i $rm_opt --pid=host --privileged --name=$DOTMESH_INNER_SERVER_NAME \
    -v /var/run/docker.sock:/var/run/docker.sock \
    -v /run/docker/plugins:/run/docker/plugins \
    -v dotmesh-boltdb:/data \
    -v $OUTER_DIR:$OUTER_DIR:rshared \
    -v $FLEXVOLUME_DRIVER_DIR:/system-flexvolume \
    $EXTRA_VOLUMES \
    $net \
    $link \
    -e "DISABLE_FLEXVOLUME=$DISABLE_FLEXVOLUME" \
    -e "PATH=$ZFS_USERLAND_ROOT/sbin:$PATH" \
    -e "LD_LIBRARY_PATH=$LD_LIBRARY_PATH" \
    -e "CONTAINER_MOUNT_PREFIX=$CONTAINER_MOUNT_PREFIX" \
    -e "MOUNT_PREFIX=$MOUNTPOINT" \
    -e "POOL=$POOL" \
    -e "YOUR_IPV4_ADDRS=$YOUR_IPV4_ADDRS" \
    -e "TRACE_ADDR=$TRACE_ADDR" \
    -e "POOL_LOGFILE=$OUTER_DIR/dotmesh_pool_log" \
    -e "DOTMESH_ETCD_ENDPOINT=$DOTMESH_ETCD_ENDPOINT" $INHERIT_ENVIRONMENT_ARGS \
    -e "LD_LIBRARY_PATH=$ZFS_USERLAND_ROOT/lib" \
    -e "ZFS_USERLAND_ROOT=$ZFS_USERLAND_ROOT" \
    $secret \
    $log_opts \
    $pki_volume_mount \
    -v dotmesh-kernel-modules:/bundled-lib \
    $DOTMESH_DOCKER_IMAGE \
    "$@" >/dev/null

RETVAL=$?

docker logs $DOTMESH_INNER_SERVER_NAME > $DIR/dotmesh_server_inner_log

exit $RETVAL
