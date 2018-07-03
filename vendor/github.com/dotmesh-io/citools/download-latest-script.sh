#!/bin/sh

# Fetch latest master into dind-cluster-original.sh

curl https://raw.githubusercontent.com/Mirantis/kubeadm-dind-cluster/master/fixed/dind-cluster-v1.10.sh > dind-cluster-original.sh

# https://github.com/Mirantis/kubeadm-dind-cluster/tree/master/fixed <-- these scripts have hard-coded docker images in them for specific k8s versions which means you don't have to rebuild kubernetes every time
