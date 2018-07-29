#!/bin/sh

mkdir -p outputs/zfs-{0.6,0.7}

docker build -f Dockerfile.0-6 -t dotmesh/zfs-userland-0.6 .
docker build -f Dockerfile.0-7 -t dotmesh/zfs-userland-0.7 .

