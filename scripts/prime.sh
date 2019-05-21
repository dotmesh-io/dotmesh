#!/usr/bin/env bash
set -e

declare -A cache

while read p; do
    read -r -a items <<< "$p"
    cache[${items[0]}]=${items[1]}
done < kubernetes/images.txt

docker pull quay.io/dotmesh/etcd:v3.0.15
docker tag quay.io/dotmesh/etcd:v3.0.15 $(hostname).local:80/dotmesh/etcd:v3.0.15
docker push $(hostname).local:80/dotmesh/etcd:v3.0.15

docker pull busybox
docker tag busybox $(hostname).local:80/busybox
docker push $(hostname).local:80/busybox

docker pull mysql:5.7.17
docker tag mysql:5.7.17 $(hostname).local:80/mysql:5.7.17
docker push $(hostname).local:80/mysql:5.7.17

# Cache images required by Kubernetes

for fq_image in "${!cache[@]}"; do
    local_name="$(hostname).local:80/${cache[$fq_image]}"
    docker pull $fq_image
    docker tag $fq_image $local_name
    docker push $local_name
done
