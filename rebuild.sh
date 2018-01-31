#!/usr/bin/env bash
set -xe
if [ -z "${SKIP_BACKEND}" ]; then
  (cd cmd/dm && ./rebuild.sh Linux)
  (cd cmd/dotmesh-server && ./rebuild.sh)
fi
if [ -z "${SKIP_FRONTEND}" ]; then
  (cd frontend && ./rebuild-testrunner.sh)
  (cd frontend && ./rebuild-gotty.sh)
  (cd frontend && ./rebuild-chromedriver.sh)
  (cd frontend && ./rebuild.sh)
  (cd billing && ./rebuild.sh)
  (cd communications && ./rebuild.sh)
fi