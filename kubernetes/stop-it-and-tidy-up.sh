#!/usr/bin/env bash
while true; do E=$(./dind-cluster-v1.7.sh clean 2>&1 |grep 'device or resource' |cut -d ':' -f 5|cut -d ' ' -f 3); sudo umount $E; done
