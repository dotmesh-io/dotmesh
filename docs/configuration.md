# Configuration

This doc should detail alllll of the variables in `require_zfs.sh`, which is used to configure dotmesh's inner container, and what they are used for. Should you add new stuff there, you should write it up here.

This may not initially be "complete", so if you find some extra variables in there, please add them here. 

Variables are written in here in the order they appear in the file.


## Auto-configured variables
| Name                 | Value | Description                                                                                                                                                                        |
|----------------------+---------+------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `KERN`                             | `$(uname -r)` | Which kernel your system is using |
| `RELEASE`                             | zfs-${KERN}.tar.gz | Which version of zfs to grab |


## User-configured variables
| Name                 | Default | Description                                                                                                                                                                        |
|----------------------+---------+------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `POOL_SIZE`                             | `10G` | Size of dotmesh zfs pool. Setting it to `AUTO` takes up most of the space, but is not advised as this is hacky and only intended for kubernetes volumes. |
| `USE_POOL_DIR`                             | `/var/lib/dotmesh` | Location to store dotmesh data. Only likely to change in automated tests |
| `USE_POOL_NAME`                             | `pool` | Name to give the pool. Only likely to change in automated tests |
| `DOTMESH_INNER_SERVER_NAME`                             | `dotmesh-server-inner` | Name to give the inner docker container |
| `FLEXVOLUME_DRIVER_DIR`                             | `/usr/libexec/kubernetes/kubelet-plugins/volume/exec` | Directory to install flexvolume driver - kubernetes specifc |
| `CONTAINER_POOL_MNT`                             | NOT SET | if set, figure out our path using some magic nsenter commands. ?? Seems to be kube related again. |
| `CONTAINER_POOL_PVC`                             | NOT SET | ?? fill in please if you know this |
| `EXTRA_HOST_COMMANDS`                             | NOT SET | Stuff to pass the nsenter command at this point which may be necessary if you are using a weird operating system, like NixOS. |
| `CONTAINER_MOUNT_REFIX`                             | `$OUTER_DIR/container_mnt` | ?? |