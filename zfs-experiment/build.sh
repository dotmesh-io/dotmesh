#!/bin/sh

mkdir outputs/zfs-{0.6,0.7}

docker build -f Dockerfile.0-6 dotmesh/zfs-userland-0.6
docker build -f Dockerfile.0-7 dotmesh/zfs-userland-0.7

