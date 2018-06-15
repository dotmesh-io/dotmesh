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

# Put the data file inside /var/lib so that we end up on the big
# partition if we're in a LinuxKit env.
POOL_SIZE=${POOL_SIZE:-10G}
DIR=${USE_POOL_DIR:-/var/lib/dotmesh}
DIR=$(echo $DIR |sed s/\#HOSTNAME\#/$(hostname)/)
FILE=${DIR}/dotmesh_data
POOL=${USE_POOL_NAME:-pool}
POOL=$(echo $POOL |sed s/\#HOSTNAME\#/$(hostname)/)
DOTMESH_INNER_SERVER_NAME=${DOTMESH_INNER_SERVER_NAME:-dotmesh-server-inner}
FLEXVOLUME_DRIVER_DIR=${FLEXVOLUME_DRIVER_DIR:-/usr/libexec/kubernetes/kubelet-plugins/volume/exec}
INHERIT_ENVIRONMENT_NAMES=( "FILESYSTEM_METADATA_TIMEOUT" "DOTMESH_UPGRADES_URL" "DOTMESH_UPGRADES_INTERVAL_SECONDS")

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

    if [ $OUTER_DIR != $DIR ]
    then
        # Make paths involving $OUTER_DIR work in OUR namespace, by binding $DIR to $OUTER_DIR
        mkdir -p $OUTER_DIR
        mount --make-rshared /
        mount --rbind $DIR $OUTER_DIR
        mount --make-rshared $OUTER_DIR
        echo "Here's the contents of $OUTER_DIR in the require_zfs.sh container:"
        ls -l $OUTER_DIR
        echo "Here's the contents of $DIR in the require_zfs.sh container:"
        ls -l $DIR
        echo "They should be the same!"
    fi
else
    OUTER_DIR="$DIR"
fi

# Set up paths we'll use for stuff
MOUNTPOINT=${MOUNTPOINT:-$OUTER_DIR/mnt}
CONTAINER_MOUNT_PREFIX=${CONTAINER_MOUNT_PREFIX:-$OUTER_DIR/container_mnt}

# Set up mounts that are needed
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
    mkdir -p $CONTAINER_MOUNT_PREFIX"

if [ ! -e /sys ]; then
    mount -t sysfs sys sys/
fi

if [ ! -d $DIR ]; then
    mkdir -p $DIR
fi

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

POOL_LOGFILE=$DIR/dotmesh_pool.log

run_in_zfs_container() {
    NAME=$1
    shift
    docker run -i --rm --pid=host --privileged --name=dotmesh-$NAME-$$ \
           -v $OUTER_DIR:$OUTER_DIR:rshared \
           $DOTMESH_DOCKER_IMAGE \
           "$@"
}

set -ex

if [ ! -e /dev/zfs ]; then
    mknod -m 660 /dev/zfs c $(cat /sys/class/misc/zfs/dev |sed 's/:/ /g')
fi
if ! run_in_zfs_container zpool-status zpool status $POOL; then

    # TODO: make case where truncate previously succeeded but zpool create
    # failed or never run recoverable.
    if [ ! -f $FILE ]; then
        truncate -s $POOL_SIZE $FILE
        run_in_zfs_container zpool-create zpool create -m $MOUNTPOINT $POOL "$OUTER_DIR/dotmesh_data"
        echo "This directory contains dotmesh data files, please leave them alone unless you know what you're doing. See github.com/dotmesh-io/dotmesh for more information." > $DIR/README
        run_in_zfs_container zpool-get zpool get -H guid $POOL |cut -f 3 > $DIR/dotmesh_pool_id
        if [ -n "$CONTAINER_POOL_PVC_NAME" ]; then
            echo "$CONTAINER_POOL_PVC_NAME" > $DIR/dotmesh_pvc_name
        fi
    else
        run_in_zfs_container zpool-import zpool import -f -d $OUTER_DIR $POOL
    fi
    echo "`date`: Pool '$POOL' mounted from host mountpoint '$OUTER_DIR', zfs mountpoint '$MOUNTPOINT'" >> $POOL_LOGFILE
else
    echo "`date`: Pool '$POOL' already exists, adopted by new dotmesh server" >> $POOL_LOGFILE
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
net="-p ${PORT}:32607 -p 32608:32608"
link=""

# this setting means we have set DOTMESH_ETCD_ENDPOINT to a known working
# endpoint and we don't want any links for --net flags passed to Docker
if [ -z "$DOTMESH_MANUAL_NETWORKING" ]; then
    if [ "$DOTMESH_ETCD_ENDPOINT" == "" ]; then
        # If etcd endpoint is overridden, then don't try to link to a local
        # dotmesh-etcd container (etcd probably is being provided externally, e.g.
        # by etcd operator on Kubernetes).
        link="--link dotmesh-etcd:dotmesh-etcd"
    fi
    if [ "$DOTMESH_ETCD_ENDPOINT" != "" ]; then
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

# Prepare cleanup logic

TERMINATING=no

cleanup() {
    local REASON="$1"

    if [ $TERMINATING = no ]
    then
        echo "`date`: Shutting down due to $REASON" >> $POOL_LOGFILE
        TERMINATING=yes
    else
        echo "`date`: Ignoring $REASON as we're already shutting down" >> $POOL_LOGFILE
        return
    fi

    if false
    then
        # Log mounts

        # '| egrep "$DIR|$OUTER_DIR"' might make this less verbose, but
        # also might miss out useful information about parent
        # mountpoints. For instaince, in dind mode in the test suite, the
        # relevent mountpoint in the host is `/dotmesh-test-pools` rather
        # than the full $OUTER_DIR.

        echo "`date`: DEBUG mounts on host:" >> $POOL_LOGFILE
        nsenter -t 1 -m -u -n -i cat /proc/self/mountinfo | sed 's/^/HOST: /' >> $POOL_LOGFILE || true
        echo "`date`: DEBUG mounts in require_zfs.sh container:" >> $POOL_LOGFILE
        cat /proc/self/mountinfo | sed 's/^/OUTER: /' >> $POOL_LOGFILE || true
        echo "`date`: DEBUG mounts in an inner container:" >> $POOL_LOGFILE
        run_in_zfs_container inspect-namespace /bin/cat /proc/self/mountinfo | sed 's/^/INNER: /' >> $POOL_LOGFILE || true
        echo "`date`: End of mount tables." >> $POOL_LOGFILE
    fi

    # Release the ZFS pool. Do so in a mount namespace which has $OUTER_DIR
    # rshared, otherwise zpool export's unmounts can be mighty confusing.

    # Step 1: Unmount the ZFS mountpoint, and any dots still inside
    echo "`date`: Unmounting $MOUNTPOINT:" >> $POOL_LOGFILE
    run_in_zfs_container zpool-unmount umount --force --recursive "$MOUNTPOINT" >> $POOL_LOGFILE 2>&1 || true

    # Step 2: Shut down the pool.
    echo "`date`: zpool exporting $POOL:" >> $POOL_LOGFILE
    run_in_zfs_container zpool-export zpool export -f "$POOL" >> $POOL_LOGFILE 2>&1

    echo "`date`: Finished cleanup: zpool export returned $?" >> $POOL_LOGFILE
}

shutdown() {
    local SIGNAL="$1"

    # Remove the handler now it's happened once
    trap - $SIGNAL

    cleanup "signal $SIGNAL"

    exit 0
}

trap 'shutdown SIGTERM' SIGTERM
trap 'shutdown SIGINT' SIGINT
trap 'shutdown SIGQUIT' SIGQUIT
trap 'shutdown SIGHUP' SIGHUP
trap 'shutdown SIGKILL' SIGKILL

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
    -v $OUTER_DIR:$OUTER_DIR:rshared \
    -v $FLEXVOLUME_DRIVER_DIR:/system-flexvolume \
    $net \
    $link \
    -e "DISABLE_FLEXVOLUME=$DISABLE_FLEXVOLUME" \
    -e "PATH=$PATH" \
    -e "LD_LIBRARY_PATH=$LD_LIBRARY_PATH" \
    -e "CONTAINER_MOUNT_PREFIX=$CONTAINER_MOUNT_PREFIX" \
    -e "MOUNT_PREFIX=$MOUNTPOINT" \
    -e "POOL=$POOL" \
    -e "YOUR_IPV4_ADDRS=$YOUR_IPV4_ADDRS" \
    -e "TRACE_ADDR=$TRACE_ADDR" \
    -e "DOTMESH_ETCD_ENDPOINT=$DOTMESH_ETCD_ENDPOINT" $INHERIT_ENVIRONMENT_ARGS \
    $secret \
    $log_opts \
    $pki_volume_mount \
    -v dotmesh-kernel-modules:/bundled-lib \
    $DOTMESH_DOCKER_IMAGE \
    "$@" >/dev/null

RETVAL=$?

cleanup "inner container terminating with retval=$RETVAL"

docker logs $DOTMESH_INNER_SERVER_NAME > $DIR/dotmesh_server_inner_log

exit $RETVAL
