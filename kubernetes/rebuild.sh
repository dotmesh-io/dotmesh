#!/bin/sh

set -ex

OUT=../yaml

mkdir -p $OUT

cp etcd-operator-clusterrole.yaml etcd-operator-dep.yaml dotmesh-etcd-cluster.yaml $OUT

if [ -z "$CI_DOCKER_TAG" ]
then
	 # Non-CI build
	 CI_DOCKER_TAG=latest
fi

sed "s/DOCKER_TAG/$CI_DOCKER_TAG/" < dotmesh.yaml > $OUT/dotmesh.yaml
sed "s_rbac.authorization.k8s.io/v1beta1_rbac.authorization.k8s.io/v1_"< $OUT/dotmesh.yaml > $OUT/dotmesh-k8s-1.8.yaml
sed "s_/usr/libexec/kubernetes/kubelet-plugins/volume/exec_/home/kubernetes/flexvolume_" < $OUT/dotmesh-k8s-1.8.yaml > $OUT/dotmesh-k8s-1.8.gke.yaml
