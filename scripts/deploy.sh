#!/bin/bash -e

# push the locally built production image to GCR using the variables defined
# in .gitlab-ci.yaml

set -e

function deploy-manifest() {
  local filename="$1"
  echo "running manifest: $filename"
  cat "/app/$filename" | envsubst
  cat "/app/$filename" | envsubst | kubectl apply -f -
}

deploy-manifest deploy/daemonset.yaml
deploy-manifest deploy/frontend-deployment.yaml
deploy-manifest deploy/billing-deployment.yaml
deploy-manifest deploy/communications-deployment.yaml

# restart docker on the dotmesh machine to get around the shared mount startup problem
NODE_ID=$(kubectl get no --selector dmserver=true -o jsonpath="{.items[].metadata.name}")
gcloud compute ssh $NODE_ID --strict-host-key-checking=no --command="sudo systemctl status docker"