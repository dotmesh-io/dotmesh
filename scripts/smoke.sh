#!/usr/bin/env bash
set -xe
set -o pipefail

function usage() {
	echo "usage: $0 path-to-dm-client dotmesh-server-image-name"
}

# Smoke test to see whether basics still work on e.g. macOS; also tests pushing
# to the dothub.

if [ -z "$1" ]
  then
    usage
	exit 1
fi

export DM="$1"
export NEW_UUID=$(cat /dev/urandom | tr -dc 'a-zA-Z0-9' | fold -w 32 | head -n 1 || true)
export VOL="volume_`date +%s`_${NEW_UUID}"
export IMAGE="${CI_DOCKER_REGISTRY:-`hostname`.local:80/dotmesh}/"$2":${CI_DOCKER_TAG:-latest}"

# Verbose output on push
export DEBUG_MODE=1

# We use a bespoke config path to isolate us from other runs (although
# we do hog the node's docker state, so it's far from perfect)

export CONFIG=/tmp/smoke_test_$$.dmconfig
export REMOTE="smoke_test_`date +%s`"

function delete_lingering_dots() {
	platform=`uname`
	DOTS=`"$DM" -c "$CONFIG" list | grep "volume" | cut -d ' ' -f 3` || true
	for dot in $DOTS
	do
		if [[ $platform == 'Linux' ]]; then
			echo "deleting $dot" && timeout 10 "$DM" -c "$CONFIG" dot delete -f $dot || true
		elif [[ $platform == 'Darwin' ]]; then
			echo "deleting $dot" && gtimeout 10 "$DM" -c "$CONFIG" dot delete -f $dot || true
		fi
	done
}



# Set traps before we go into the subshells, otherwise they'll never be
# triggered!
function finish {
    if [ SUCCESS = false ]; then
        echo "INTERNAL STATE"
        "$DM" -c "$CONFIG" remote switch local
        "$DM" -c "$CONFIG" debug DotmeshRPC.DumpInternalState
    fi
    if [ x$SMOKE_TEST_REMOTE != x ]; then
        "$DM" -c "$CONFIG" remote rm "$REMOTE" || true
    fi

	"$DM" -c "$CONFIG" dot delete -f $VOL
	delete_lingering_dots
	"$DM" -c "$CONFIG" list
    rm "$CONFIG" || true
}

trap finish EXIT

((

sudo "$DM" -c "$CONFIG" cluster reset || (sleep 10; sudo "$DM" cluster reset) || true

echo "### Installing image ${IMAGE}"

"$DM" -c "$CONFIG" cluster init --offline --image "$IMAGE"

echo "### Testing docker run..."

docker run --rm -i --name smoke -v "$VOL:/foo" --volume-driver dm ubuntu touch /foo/X

echo "### Testing list..."

LIST=`"$DM" -c "$CONFIG" list`

if [[ $LIST == *"$VOL"* ]]; then
    echo "String '$VOL' found, yay!"
else
    echo "String '$VOL' not found, boo :("
    exit 1
fi

echo "### Testing commit..."

"$DM" -c "$CONFIG" switch "$VOL"
"$DM" -c "$CONFIG" commit -m 'Test commit'

sleep 5

OUT=`"$DM" -c "$CONFIG" log`

if [[ $OUT == *"Test commit"* ]]; then
    echo "Commit found, yay!"
    SUCCESS=true
else
    echo "Commit not found, boo :("
    SUCCESS=false
    exit 1
fi

# This variable is set up in GitLab.
if [ x$SMOKE_TEST_REMOTE != x ]
then
    echo "### Testing push to remote..."

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
fi) 2>&1 ) | ts

exit 0
