#!/bin/sh

# Fetch latest master into dind-cluster-original.sh

curl https://raw.githubusercontent.com/kubernetes-sigs/kubeadm-dind-cluster/master/dind-cluster.sh > dind-cluster-original.sh

# https://github.com/Mirantis/kubeadm-dind-cluster/tree/master/fixed <-- these scripts have hard-coded docker images in them for specific k8s versions which means you don't have to rebuild kubernetes every time
