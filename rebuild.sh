#!/usr/bin/env bash
set -xe
(cd cmd/dm && ./rebuild.sh Linux)
(cd cmd/dotmesh-server && ./rebuild.sh)
(cd kubernetes && ./rebuild.sh)