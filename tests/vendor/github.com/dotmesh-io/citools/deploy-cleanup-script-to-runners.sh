set -x
for i in jogger runner swimmer trundler pootler stroller skipper bouncer roller dasher dancer prancer racer pacer hurdler vaulter jumper leaper ambler
do
    HOST=gitlab-$i.dotmesh.io
    echo "Deploying cleanup-old-test-resources.go and cronjob to $HOST..."
    SSH_TARGET=gitlab-runner@$HOST
    scp cmd/cleanup-old-test-resources/main.go $SSH_TARGET:cleanup-old-test-resources.go
    ssh $SSH_TARGET sudo chown -R gitlab-runner:gitlab-runner /home/gitlab-runner
    # convention for deploying new versions of zumount is re-running this script
    ssh $SSH_TARGET 'sudo curl -sSL -o /usr/local/bin/zumount.$$ https://get.dotmesh.io/zumount && sudo mv /usr/local/bin/zumount.$$ /usr/local/bin/zumount && sudo chmod +x /usr/local/bin/zumount'
    ssh $SSH_TARGET 'sudo crontab -' <<EOF
# Please don't hand-edit this crontab, use citools/deploy-cleanup-script-to-runners.sh to keep
# all the runners in sync (and if that annoys you, then install chef/puppet/etc to manage them properly)

PATH = /usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin:/usr/games:/usr/local/games:/snap/bin:/home/gitlab-runner/go/bin:/home/gitlab-runner/bin

* * * * * (go run /home/gitlab-runner/cleanup-old-test-resources.go 2>&1 | ts) >> /home/gitlab-runner/cleanup-old-test-resources.log
* * * * * (echo -n "zpool_total "; echo \$(( \$(zpool list |wc -l) - 1 ))) |sponge /var/lib/prometheus/node-exporter/zpool_total.prom
@daily docker system prune -f ; chown -R gitlab-runner:gitlab-runner /home/gitlab-runner/ ; journalctl --vacuum-time=10d
EOF
done
