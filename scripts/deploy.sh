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
