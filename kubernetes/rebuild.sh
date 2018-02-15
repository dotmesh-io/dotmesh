#!/bin/sh

OUT=../yaml

mkdir -p $OUT

cp etcd-operator-clusterrole.yaml etcd-operator-dep.yaml $OUT

if [ -z "$CI_DOCKER_TAG" ]
then
	 # Non-CI build
	 CI_DOCKER_TAG=latest
fi

for YAML in dotmesh.yaml dotmesh-k8s-1.8.yaml dotmesh-k8s-1.7.yaml
do
	 sed "s/DOCKER_TAG/$CI_DOCKER_TAG/" < $YAML > $OUT/$YAML
done
