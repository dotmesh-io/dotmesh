#!/bin/bash
set -xe

# Prepare cleanup logic

TERMINATING=no

cleanup() {
    local REASON="$1"

    if [ -z "$CONTAINER_POOL_MNT" ]; then
        echo "Skipping shutdown actions in local mode"
        return
    fi

    if [ $TERMINATING = no ]
    then
        echo "Shutting down due to $REASON"
        TERMINATING=yes
    else
        echo "Ignoring $REASON as we're already shutting down"
        return
    fi

    if true
    then
        # Log mounts

        # '| egrep "$DIR|$OUTER_DIR"' might make this less verbose, but
        # also might miss out useful information about parent
        # mountpoints. For instaince, in dind mode in the test suite, the
        # relevent mountpoint in the host is `/dotmesh-test-pools` rather
        # than the full $OUTER_DIR.

        echo "DEBUG mounts on host:"
        nsenter -t 1 -m -u -n -i cat /proc/self/mountinfo | sed 's/^/HOST: /' || true
        echo "DEBUG mounts in require_zfs.sh container:"
        cat /proc/self/mountinfo | sed 's/^/OUTER: /' || true
        echo "DEBUG mounts in an inner container:"
        run_in_zfs_container inspect-namespace /bin/cat /proc/self/mountinfo | sed 's/^/INNER: /' || true
        echo "DEBUG End of mount tables."
    fi

    # Release the ZFS pool. Do so in a mount namespace which has $OUTER_DIR
    # rshared, otherwise zpool export's unmounts can be mighty confusing.

    # Step 1: Unmount any dots and the ZFS mountpoint (if it's there)
    echo "Unmount dots in $MOUNTPOINT/mnt/dmfs:"
    run_in_zfs_container zpool-unmount-dots sh -c "cd \"$MOUNTPOINT/mnt/dmfs\" && for i in *; do umount --force \$i; done" || true
    echo "Unmounting $MOUNTPOINT:"
    run_in_zfs_container zpool-unmount umount --force --recursive "$MOUNTPOINT"|| true

    # Step 2: Shut down the pool.
    echo "zpool exporting $POOL:"
    if run_in_zfs_container zpool-export zpool export -f "$POOL"
    then
        echo "`date`: Exported pool OK" >> $POOL_LOGFILE
        echo "Exported pool OK."
    else
        echo "`date`: ERROR: Failed exporting pool!" >> $POOL_LOGFILE
        echo "ERROR: Failed exporting pool!."
    fi
}

shutdown() {
    local SIGNAL="$1"

    # Remove the handler now it's happened once
    trap - $SIGNAL

    cleanup "signal $SIGNAL"
}

trap 'shutdown SIGTERM' SIGTERM
trap 'shutdown SIGINT' SIGINT
trap 'shutdown SIGQUIT' SIGQUIT
trap 'shutdown SIGHUP' SIGHUP

RETVAL=$?

exit $RETVAL
