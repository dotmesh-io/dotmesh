#!/bin/bash
for i in jogger runner swimmer trundler pootler stroller skipper bouncer roller dasher dancer prancer racer pacer hurdler vaulter jumper leaper ambler; do
    HOST=gitlab-$i.dotmesh.io
    SSH_TARGET=gitlab-runner@$HOST
    echo $X
    ssh $SSH_TARGET sudo sed -i 's/concurrent = 2/concurrent = 1/'
    ssh $SSH_TARGET sudo gitlab-runner restart
done
