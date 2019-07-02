#!/bin/bash

function timeout_handler
{
   echo "Allowable time for execution exceeded in pid $BASHPID, stopping"
   exit 1
}

function allow_time
{
    (
        sleep $1
        echo "Timed out, terminating pid $2"
        kill $2
    ) &
}

# 30 minutes, in seconds
TIMEOUT=1800

# Flag all stale old test run resources as finished

for DIR in `find /dotmesh-test-pools -maxdepth 1 -ctime +1`
do
    echo "Marking $DIR for cleanup because it's stale..."
    touch $DIR/finished
done

# Remove all finished test run resources

for f in `find /dotmesh-test-pools -maxdepth 2 -name finished `
do
    DIR="`echo $f | sed s_/finished__`"

    # Run all the cleanups in parallel, but with a non-blocking lock
    # in case we've already started it in a previous run and it's just
    # still running
    (
        flock -n 9 && (
            echo "Cleaning up $DIR in pid $BASHPID..."
            allow_time $TIMEOUT $BASHPID
            trap timeout_handler SIGALRM

            for CMD in `find "$DIR" -name cleanup-actions.\* -print | sort`
            do
                # Run scripts and remove them when done, but break the loop if any fail
                sh -ex $CMD && rm $CMD || break
            done
#            rm -rf "$DIR" || true

            # Terminate timeout process
            kill $!
            echo "Finished cleaning up $DIR in pid $BASHPID."
        ) || echo "Skipped $DIR because it's locked"
    ) 9> $DIR/cleanup-initiated &
done

wait
