for i in gitlab-jogger.dotmesh.io gitlab-runner.dotmesh.io gitlab-swimmer.dotmesh.io gitlab-trundler.dotmesh.io gitlab-pootler.dotmesh.io gitlab-stroller.dotmesh.io gitlab-skipper.dotmesh.io gitlab-bouncer.dotmesh.io gitlab-roller.dotmesh.io
do
    echo "Deploying cleanup-old-test-resources.go and cronjob to $i..."
    SSH_TARGET=gitlab-runner@$i
    scp cleanup-old-test-resources.go $SSH_TARGET:cleanup-old-test-resources.go
    ssh $SSH_TARGET 'sudo add-apt-repository ppa:hnakamur/golang-1.10 ; sudo apt-get -u update ; sudo apt-get install -y golang-go; sudo crontab -' <<EOF
# Please don't hand-edit this crontab, use citools/deploy-cleanup-script-to-runners.sh to keep
# all the runners in sync (and if that annoys you, then install chef/puppet/etc to manage them properly)

PATH = /usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin:/usr/games:/usr/local/games:/snap/bin:/home/gitlab-runner/go/bin:/home/gitlab-runner/bin

@daily rm -f /etc/zfs/zpool.cache && /sbin/reboot -f
@reboot rm -rf /dotmesh-test-pools
* * * * * (go run /home/gitlab-runner/cleanup-old-test-resources.go 2>&1 | ts) >> /home/gitlab-runner/cleanup-old-test-resources.log
EOF
done
