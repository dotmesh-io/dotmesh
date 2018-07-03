#!/usr/bin/env bash
set -xe
(cd cmd/dm && ./rebuild.sh Linux)
(./rebuild_server.sh)
(cd kubernetes && ./rebuild.sh)