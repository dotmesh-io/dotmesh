#!/bin/sh

# Generate dind-cluster.sh.patch from the differences between dind-cluster-original.sh and dind-cluster-patched.sh

diff -u dind-cluster-original.sh dind-cluster-patched.sh > dind-cluster.sh.patch
