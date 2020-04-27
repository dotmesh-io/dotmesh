#!/bin/sh
docker build -t quay.io/dotmesh/kubeadm-dind-cluster:v1.10 .
docker push quay.io/dotmesh/kubeadm-dind-cluster:v1.10
