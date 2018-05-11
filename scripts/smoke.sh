#!/usr/bin/env bash
set -xe

# Smoke test to see whether basics still work on e.g. macOS; also tests the 

DM="$1"
VOL="volume_`date +%s`"
IMAGE="${CI_DOCKER_REGISTRY:-`hostname`.local:80/dotmesh}/"$2":${CI_DOCKER_TAG:-latest}"

# We use a bespoke config path to isolate us from other runs (although
# we do hog the node's docker state, so it's far from perfect)

CONFIG=/tmp/smoke_test_$$.dmconfig
trap 'rm "$CONFIG" || true' EXIT

sudo "$DM" -c "$CONFIG" cluster reset || (sleep 30; sudo "$DM" cluster reset) || true

echo "### Installing image ${IMAGE}"

"$DM" -c "$CONFIG" cluster init --offline --image "$IMAGE"

echo "### Testing docker run..."

docker run --rm -i --name smoke -v "$VOL:/foo" --volume-driver dm ubuntu touch /foo/X

echo "### Testing list..."

OUT=`"$DM" -c "$CONFIG" list`

if [[ $OUT == *"$VOL"* ]]; then
    echo "String '$VOL' found, yay!"
else
    echo "String '$VOL' not found, boo :("
    exit 1
fi

echo "### Testing commit..."

"$DM" -c "$CONFIG" switch "$VOL"
"$DM" -c "$CONFIG" commit -m 'Test commit'

OUT=`"$DM" -c "$CONFIG" log`

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

    (set +x; echo "$SMOKE_TEST_APIKEY"; set -x) | "$DM" -c "$CONFIG" remote add "$REMOTE" "$SMOKE_TEST_REMOTE"

    "$DM" -c "$CONFIG" push "$REMOTE" "$VOL"

    "$DM" -c "$CONFIG" remote switch "$REMOTE"

    REMOTE_NAME="`echo $SMOKE_TEST_REMOTE | sed s/@.*$//`"

    for TRY in `seq 10`; do
        if "$DM" -c "$CONFIG" dot show "$REMOTE_NAME"/"$VOL"; then
            echo "Found $REMOTE_NAME/$VOL"
            break
        else
            echo "$REMOTE_NAME/$VOL not found, retrying ($TRY)..."
            sleep 1
        fi
    done

    echo "### Testing delete on remote..."

    "$DM" -c "$CONFIG" dot delete -f "$REMOTE_NAME"/"$VOL"

    "$DM" -c "$CONFIG" remote switch local
    "$DM" -c "$CONFIG" remote rm "$REMOTE"
fi

exit 0
