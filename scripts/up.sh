#!/usr/bin/env bash
if [ "$1" == "" ]; then
    echo "Upgrades dotmesh on N nodes, assuming hostnames are in form <node-prefix><N>."
    echo "Usage: ./up.sh <node-prefix> <N> <arguments-passed-to-upgrade>"
    exit 1
fi
NODE_PREFIX=$1; shift
N=$1; shift
REMAINDER=$@
for X in `seq 1 $N`; do
    ssh ${NODE_PREFIX}${X} dm cluster upgrade $@ &
done
for P in `jobs -p`; do
    wait $P
done
