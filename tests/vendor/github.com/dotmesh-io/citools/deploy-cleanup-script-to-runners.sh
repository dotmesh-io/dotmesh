for i in gitlab-hopper.dotmesh.io gitlab-jogger.dotmesh.io gitlab-runner.dotmesh.io gitlab-swimmer.dotmesh.io
do
    echo "Deploying cleanup-old-test-resources.sh and cronjob to $i..."
    SSH_TARGET=gitlab-runner@$i
    scp cleanup-old-test-resources.sh $SSH_TARGET:cleanup-old-test-resources.sh
    ssh $SSH_TARGET 'if sudo crontab -l | grep cleanup-old-test-resources.sh; then echo already done; else (sudo crontab -l ; echo "*/5 * * * * /home/gitlab-runner/cleanup-old-test-resources.sh > /home/gitlab-runner/cleanup-old-test-resources.log") | sudo crontab -; fi'
done
