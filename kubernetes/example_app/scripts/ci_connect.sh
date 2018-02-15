#!/bin/bash -e
set -e

export DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

if [ -z "${GCLOUD_SERVICE_KEY}" ]; then
  echo >&2 "GCLOUD_SERVICE_KEY needed"
  exit 1
fi
if [ -z "${GCP_PROJECT_ID}" ]; then
  echo >&2 "GCP_PROJECT_ID needed"
  exit 1
fi
if [ -z "${GCP_ZONE}" ]; then
  echo >&2 "GCP_ZONE needed"
  exit 1
fi
if [ -z "${GCP_CLUSTER_ID}" ]; then
  echo >&2 "GCP_CLUSTER_ID needed"
  exit 1
fi
echo $GCLOUD_SERVICE_KEY | base64 -d > ${HOME}/gcloud-service-key.json
echo "activating gcloud service account"
gcloud auth activate-service-account --key-file ${HOME}/gcloud-service-key.json
echo "set gcloud project $GCP_PROJECT_ID"
gcloud config set project $GCP_PROJECT_ID
echo "connect to container cluster $GCP_CLUSTER_ID in $GCP_ZONE"
gcloud container clusters get-credentials --zone $GCP_ZONE $GCP_CLUSTER_ID
