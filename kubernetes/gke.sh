#!/usr/bin/env bash

set -e

export DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

FLEX_VOLUME_FOLDER="/usr/libexec/kubernetes/kubelet-plugins/volume/exec/datamesh.io~dm"
DATAMESH_CONFIG_FOLDER="/root/.datamesh"
PASSWORD="secret123"
CONFIG='{"CurrentRemote":"gke","Remotes":{"Local":{"User":"admin","Hostname":"127.0.0.1","ApiKey":"secret123","CurrentVolume":null,"CurrentBranches":null,"DefaultRemoteVolumes":null}}}'
GKE_K8S_VERSION="1.7.11-gke.1"

COMMAND="$1"

if [ "$COMMAND" == "create" ]; then

  echo "creating k8s cluster"
  gcloud container clusters create testdm \
    --image-type=ubuntu \
    --tags=datamesh \
    --cluster-version=$GKE_K8S_VERSION

  echo "opening firewall"
  gcloud compute firewall-rules create datamesh --allow tcp:6969 --target-tags=datamesh || true

elif [ "$COMMAND" == "delete" ]; then

  echo "delete firewall rule"
  gcloud compute firewall-rules delete datamesh

  echo "delete k8s cluster"
  gcloud container clusters delete testdm

elif [ "$COMMAND" == "prepare" ]; then

  echo "preparing nodes"
  for node in $(kubectl get no | tail -n +2 | awk '{print $1}'); do

    echo "prepare node $node"

    # create folders
    gcloud compute ssh $node --command "sudo mkdir -p $FLEX_VOLUME_FOLDER"
    gcloud compute ssh $node --command "sudo mkdir -p $DATAMESH_CONFIG_FOLDER"

    # copy flexvolume binary
    gcloud compute ssh $node --command "sudo docker run --rm -v $FLEX_VOLUME_FOLDER:/hostfolder binocarlos/k8s-datamesh:v2 sh -c 'cp -f /usr/local/bin/flexvolume /hostfolder/dm'"

    # write host config
    echo "$CONFIG" | gcloud compute ssh $node --command "cat > /tmp/datamesh-config"
    gcloud compute ssh $node --command "sudo mv /tmp/datamesh-config $DATAMESH_CONFIG_FOLDER/config"

    gcloud compute ssh $node --command "sudo systemctl restart kubelet"
    gcloud compute ssh $node --command "sudo journalctl -u kubelet | grep flex"
  done

elif [ "$COMMAND" == "deploy" ]; then

  echo "installing datamesh"
  kubectl create namespace datamesh
  echo "$PASSWORD" > datamesh-admin-password.txt
  kubectl create secret generic datamesh --from-file=datamesh-admin-password.txt -n datamesh
  rm -f datamesh-admin-password.txt
  kubectl apply -f manifests/etcd-rbac.yaml
  kubectl apply -f manifests/etcd-operator.yaml
  sleep 30 # give the operator time
  kubectl apply -f manifests/daemonset.yaml
  kubectl apply -f manifests/dynamic-provisioner.yaml

  NODE_IP=$(kubectl get no -o wide | awk '{print $6}' | tail -n 1)

  echo "##################################################"
  echo "#"
  echo "# Your datamesh cluster has been installed"
  echo "#"
  echo "# DATAMESH_PASSWORD=$PASSWORD dm remote add gke admin@$NODE_IP"
  echo "#"

else

  echo "usage: gke.sh <command>"
  echo
  echo "commands:"
  echo
  echo "  create - create a new k8s cluster"
  echo "  delete - delete the cluster"
  echo "  prepare - prepare the cluster"
  echo "  start - start the cluster"

fi



