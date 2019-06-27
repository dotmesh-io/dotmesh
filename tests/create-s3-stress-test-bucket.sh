#!/bin/sh

set -e

BUCKET="s3://dotmesh-transfer-stress-test"
COUNT=100000

echo "This script will set up the bucket used by TestS3Stress (in s3remote_test.go). It only needs to be run once ever unless we lose the bucket, the bucket is used read-only by the test."
echo "The bucket is called $BUCKET"

if [ x$S3_SECRET_KEY == x ]
then
    echo "S3_SECRET_KEY must be set to, well, an S3 secret key"
    exit 1
fi

if [ x$S3_ACCESS_KEY == x ]
then
    echo "S3_ACCESS_KEY must be set to, well, an S3 secret key"
    exit 1
fi

run_s3cmd() {
    s3cmd --access_key=$S3_ACCESS_KEY --secret_key=$S3_SECRET_KEY "$@"
}

TMP=${TEMP:-.}
TMPDIR=$TMP/create-s3-stress-test-bucket.$$
mkdir $TMPDIR

echo "Using $TMPDIR for staging..."

echo "Creating 16GiB file..."
dd if=/dev/zero of=$TMPDIR/large-file bs=1M count=16K

echo "Uploading..."
run_s3cmd put $TMPDIR/large-file $BUCKET/large-file

echo "Uploading $COUNT 1KiB files..."
COUNTER=0
while [ $COUNTER -lt $COUNT ]
do
    echo "File $COUNTER" > $TMPDIR/small-file
    run_s3cmd put $TMPDIR/small-file $BUCKET/small-file-$COUNTER
    ((COUNTER++)) || true
done

# Turns out the S3 integration only creates DM commits for versions it
# finds *since* the initial pull. Therefore, a test that pulls lots of
# commits would need to create lots of versions on every run (between
# initial clone and pull), which will be *very* time consuming, so
# we'll save that for later; and there's no point in creating a file
# with a long history as part of the read-only test bucket.

# echo "Uploading a $COUNT versions of a file..."
# COUNTER=0
# while [ $COUNTER -lt $COUNT ]
# do
#     echo "Version $COUNTER" > $TMPDIR/historical-file
#     run_s3cmd put $TMPDIR/historical-file $BUCKET/historical-file
#     ((COUNTER++)) || true
# done

echo "Done. Final result:"
run_s3cmd info $BUCKET || true
run_s3cmd ls $BUCKET || true
run_s3cmd du $BUCKET || true

rm -rf $TMPDIR
