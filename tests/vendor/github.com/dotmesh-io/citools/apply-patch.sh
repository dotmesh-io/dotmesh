#!/bin/sh

# Generate dind-cluster-patched.sh from dind-cluster-original.sh by applying dind-cluster.sh.patch
patch -o dind-cluster-patched.sh dind-cluster-original.sh dind-cluster.sh.patch
