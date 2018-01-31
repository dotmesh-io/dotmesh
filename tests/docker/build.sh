#!/usr/bin/env bash
docker build -t quay.io/lukemarsden/kubeadm-dind-cluster:v1.7-hostport .
docker push quay.io/lukemarsden/kubeadm-dind-cluster:v1.7-hostport
