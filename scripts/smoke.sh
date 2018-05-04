#!/usr/bin/env bash
set -xe

# Smoke test to see whether basics still work on e.g. macOS

DM="$1"
VOL="volume_`date +%s`"
IMAGE="${CI_DOCKER_REGISTRY:-`hostname`.local:80/dotmesh}/"$2":${CI_DOCKER_TAG:-latest}"

sudo "$DM" cluster reset || (sleep 30; sudo "$DM" cluster reset) || true

echo "### Installing image ${IMAGE}"

"$DM" cluster init --offline --image "$IMAGE"

echo "### Testing docker run..."

docker run --rm -i --name smoke -v "$VOL:/foo" --volume-driver dm ubuntu touch /foo/X

echo "### Testing list..."

OUT=`"$DM" list`

if [[ $OUT == *"$VOL"* ]]; then
    echo "String '$VOL' found, yay!"
else
    echo "String '$VOL' not found, boo :("
    exit 1
fi

echo "### Testing commit..."

"$DM" switch "$VOL"
"$DM" commit -m 'Test commit'

OUT=`"$DM" log`

if [[ $OUT == *"Test commit"* ]]; then
    echo "Commit found, yay!"
else
    echo "Commit not found, boo :("
    exit 1
fi

if [ x$SMOKE_TEST_REMOTE != x ]
then
    echo "### Testing push to remote..."
    REMOTE="smoke_test_`date +%s`"
    echo "$SMOKE_TEST_APIKEY" | "$DM" remote add "$REMOTE" "$SMOKE_TEST_REMOTE"

    "$DM" push "$REMOTE" "$VOL"

    "$DM" remote switch "$REMOTE"
    OUT=`"$DM" list`

    if [[ $OUT == *"$VOL"* ]]; then
        echo "String '$VOL' found on the remote, yay!"
    else
        echo "String '$VOL' not found on the remote, boo :("
        exit 1
    fi

    "$DM" remote switch local
    "$DM" remote rm "$REMOTE"
fi

exit 0
