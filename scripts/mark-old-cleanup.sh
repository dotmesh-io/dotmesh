#!/usr/bin/env bash
set -xe
for X in $(docker ps --format "{{.Names}}" | grep cluster- || true); do
    # don't clean up containers that have been running for < 1 hour (that have
    # runtimes in seconds or minutes)
    old=$(docker ps --filter Name=$X --format "{{.RunningFor}}" |grep 'hours\|days\|weeks\|months\|years' || true)
    # if recent is empty string, i.e. container is > 1 hour old
    if [ ! -z "$old" ]; then
        docker exec -i $X touch /CLEAN_ME_UP
    fi
done
