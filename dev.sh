#!/usr/bin/env bash -e
#
# scripts to manage the developers local installation of dotmesh
#

set -e

export DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

export DATABASE_ID=${DATABASE_ID:=""}

export DOTMESH_HOME=${DOTMESH_HOME:="~/.dotmesh"}

export CI_DOCKER_SERVER_IMAGE=${CI_DOCKER_SERVER_IMAGE:=$(hostname).local:80/dotmesh/dotmesh-server}
export BILLING_IMAGE=${BILLING_IMAGE:="$(hostname).local:80/dotmesh/dotmesh-billing"}
export COMMUNICATIONS_IMAGE=${COMMUNICATIONS_IMAGE:="$(hostname).local:80/dotmesh/dotmesh-communications"}
export FRONTEND_IMAGE=${FRONTEND_IMAGE:="$(hostname).local:80/dotmesh/dotmesh-frontend"}
export FRONTEND_BUILDER_IMAGE="$FRONTEND_IMAGE-builder"
export CHROME_DRIVER_IMAGE=${CHROME_DRIVER_IMAGE:="blueimp/chromedriver"}
export NIGHTWATCH_IMAGE=${NIGHTWATCH_IMAGE:="$(hostname).local:80/dotmesh/dotmesh-nightwatch"}
export GOTTY_IMAGE=${GOTTY_IMAGE:="$(hostname).local:80/dotmesh/dotmesh-gotty"}
export ROUTER_IMAGE=${ROUTER_IMAGE:="binocarlos/noxy"}


export DOTMESH_ROUTER_NAME=${DOTMESH_ROUTER_NAME:="dotmesh-router"}
export DOTMESH_SERVER_NAME=${DOTMESH_SERVER_NAME:="dotmesh-server-inner"}
export DOTMESH_BILLING_NAME=${DOTMESH_BILLING_NAME:="dotmesh-billing"}
export DOTMESH_COMMUNICATIONS_NAME=${DOTMESH_COMMUNICATIONS_NAME:="dotmesh-communications"}
export DOTMESH_FRONTEND_NAME=${DOTMESH_FRONTEND_NAME:="dotmesh-frontend"}
export GOTTY_NAME=${GOTTY_NAME:="dotmesh-gotty"}
export NIGHTWATCH_NAME=${NIGHTWATCH_NAME:="dotmesh-nightwatch"}
export CHROME_DRIVER_NAME=${CHROME_DRIVER_NAME:="dotmesh-chromedriver"}

export DOTMESH_SERVER_PORT=${DOTMESH_SERVER_PORT:="6969"}
export DOTMESH_FRONTEND_PORT=${DOTMESH_FRONTEND_PORT:="8080"}
export DOTMESH_BILLING_PORT=${DOTMESH_BILLING_PORT:="8088"}
export GOTTY_PORT=${GOTTY_PORT:="8081"}

export DEV_NETWORK_NAME=${DEV_NETWORK_NAME:="dotmesh-dev"}

export BILLING_CORS_DOMAINS=${BILLING_CORS_DOMAINS:="http://localhost:8080,http://dotmesh-frontend"}

export VAGRANT_HOME=${VAGRANT_HOME:="/vagrant"}
export VAGRANT_GOPATH=${VAGRANT_GOPATH:="$GOPATH/src/github.com/dotmesh-io/dotmesh"}

# Use the binary that we just built, rather than one that's lying around on
# your system.
export DM="binaries/$(uname -s)/dm"

#
#
# CLI
#
#

function cli-build() {
  echo "building dotmesh CLI binary"
  if [ -n "${GOOS}" ]; then
    cd "${DIR}/cmd/dm" && bash rebuild_docker.sh
  else
    cd "${DIR}/cmd/dm" && GOOS=linux bash rebuild_docker.sh && GOOS=darwin bash rebuild_docker.sh
  fi
  echo
  echo "dm binary created - copy it to /usr/local/bin with this command:"
  echo
  echo "sudo cp -f ./binaries/\$(uname -s |tr \"[:upper:]\" \"[:lower:]\")/dm /usr/local/bin"
}

#
#
# CLUSTER
#
#

function cluster-build() {
  cd "${DIR}/cmd/dotmesh-server" && IMAGE="${CI_DOCKER_SERVER_IMAGE}" NO_PUSH=1 bash rebuild.sh
}

function cluster-prodbuild() {
  echo "building production dotmesh server image: ${CI_DOCKER_SERVER_IMAGE}"
  cp -r "${DIR}/frontend/dist" "${DIR}/cmd/dotmesh-server/dist"
  cd "${DIR}/cmd/dotmesh-server" && IMAGE="${CI_DOCKER_SERVER_IMAGE}" NO_PUSH=1 bash mergebuild.sh
  rm -rf "${DIR}/cmd/dotmesh-server/dist"
}

function get-billing-url() {
  if [ -n "$LOCAL_BILLING" ]; then
    if [ -z "$BILLING_URL" ]; then
      BILLING_URL="http://localhost:${DOTMESH_BILLING_PORT}/api/v1"
    fi
  fi
  echo "$BILLING_URL"
}

function cluster-start() {
  echo "creating cluster using: ${CI_DOCKER_SERVER_IMAGE}"
  export BILLING_URL=$(get-billing-url)
  if [ -n "$BILLING_URL" ]; then
    echo "using billing server: ${BILLING_URL}"
  fi
  $DM cluster init \
    --image ${CI_DOCKER_SERVER_IMAGE} \
    --offline
  sleep 2
  docker network create $DEV_NETWORK_NAME &>/dev/null || true
  docker network connect $DEV_NETWORK_NAME $DOTMESH_SERVER_NAME
}

function cluster-stop() {
  docker rm -f dotmesh-server-inner
  docker rm -f dotmesh-server
  docker rm -f dotmesh-etcd
}

function cluster-upgrade() {
  echo "upgrading cluster using ${CI_DOCKER_SERVER_IMAGE}"
  export BILLING_URL=$(get-billing-url)
  if [ -n "$BILLING_URL" ]; then
    echo "using billing server: ${BILLING_URL}"
  fi
  $DM cluster upgrade \
    --image ${CI_DOCKER_SERVER_IMAGE} \
    --offline
  sleep 2
  docker network create $DEV_NETWORK_NAME &>/dev/null || true
  docker network connect $DEV_NETWORK_NAME $DOTMESH_SERVER_NAME
}

#
#
# BILLING
#
#


function billing-build() {
  echo "building dotmesh billing image: ${BILLING_IMAGE}"
  docker build -t ${BILLING_IMAGE} ${DIR}/billing
}

function billing-start() {
  if [ -n "${CLI}" ]; then
    flags=" --rm -ti --entrypoint sh"
  else
    flags=" -d"
  fi
  local ADMIN_API_KEY=$(admin-api-key)
  if [ -z "$ADMIN_API_KEY" ]; then
    echo >&2 "error no admin api key found - have you done 'make cluster.start'?"
    exit 1
  fi
  if [ -f "$DIR/credentials.env" ]; then
    source "$DIR/credentials.env"
  fi
  if [ -z "$STRIPE_PUBLIC_KEY" ]; then
    echo >&2 "error no STRIPE_PUBLIC_KEY found - please create a credentials.env file"
    exit 1
  fi
  if [ -z "$STRIPE_SECRET_KEY" ]; then
    echo >&2 "error no STRIPE_SECRET_KEY found - please create a credentials.env file"
    exit 1
  fi

  echo "running billing dev server using ${BILLING_IMAGE}"
  echo "using credentials file $DIR/credentials.env"
  echo "using admin api key: $ADMIN_API_KEY"
  echo "using CORS domains: $BILLING_CORS_DOMAINS"
  echo "using stripe public key: $STRIPE_PUBLIC_KEY"
  echo "using stripe secret key: $STRIPE_SECRET_KEY"

  local ANALYTICS_ENV=""
  if [ -n "$LOCAL_ANALYTICS" ]; then
    if [ -z "$SEGMENT_API_KEY" ]; then
      echo >&2 "error no SEGMENT_API_KEY found - please create a credentials.env file"
      exit 1
    fi
    echo "using segment api key: $SEGMENT_API_KEY"
    ANALYTICS_ENV=" -e SEGMENT_API_KEY=$SEGMENT_API_KEY"
  fi
  docker run ${flags} \
    --name ${DOTMESH_BILLING_NAME} \
    --net dotmesh-dev \
    -e "DOTMESH_SERVER_HOSTNAME=$DOTMESH_SERVER_NAME" \
    -e "DOTMESH_SERVER_PORT=$DOTMESH_SERVER_PORT" \
    -e "DOTMESH_SERVER_API_KEY=$ADMIN_API_KEY" \
    -e "COMMUNICATIONS_SERVER_HOSTNAME=$DOTMESH_COMMUNICATIONS_NAME" \
    -e "COMMUNICATIONS_SERVER_PORT=80" \
    -e "COMMUNICATIONS_ACTIVE=$LOCAL_COMMUNICATIONS" \
    -e "STRIPE_PUBLIC_KEY=$STRIPE_PUBLIC_KEY" \
    -e "STRIPE_SECRET_KEY=$STRIPE_SECRET_KEY" \
    -e "STRIPE_SIGNATURE_SECRET=$STRIPE_SIGNATURE_SECRET" \
    -e "STRIPE_TEST_WEBHOOKS_ENABLED=1" \
    -e "CORS_DOMAINS=$BILLING_CORS_DOMAINS" \
    -e "NODE_ENV=development" $ANALYTICS_ENV \
    -e "BLACKHOLE_DATA_PATH=/tmp" \
    -p ${DOTMESH_BILLING_PORT}:80 \
    -v "${DIR}/billing:/app" \
    -v "/app/node_modules/" \
    ${BILLING_IMAGE}
}

function billing-url() {
  echo "running localtunnel connected to billing"
  echo
  echo "--------------------------------------------------"
  echo "append \"/webhook\" to this url then paste it into stripe"
  echo "-------------------------------------------------"
  echo
  docker run --rm \
    --link ${DOTMESH_BILLING_NAME}:billing \
    msparks/localtunnel \
    --local-host billing \
    --port 80
}

function billing-stop() {
  echo "stopping billing dev server"
  docker rm -f ${DOTMESH_BILLING_NAME}
}

#
#
# COMMUNICATIONS
#
#

function communications-build() {
  echo "building dotmesh communications image: ${COMMUNICATIONS_IMAGE}"
  docker build -t ${COMMUNICATIONS_IMAGE} ${DIR}/communications
}

function communications-start() {
  if [ -n "${CLI}" ]; then
    flags=" --rm -ti --entrypoint sh"
  else
    flags=" -d"
  fi
  if [ -f "$DIR/credentials.env" ]; then
    source "$DIR/credentials.env"
  fi
  if [ -z "${MAILGUN_API_KEY}" ]; then
    echo >&2 "error no MAILGUN_API_KEY found - please create a credentials.env file"
    exit 1
  fi
  if [ -z "${MAILGUN_DOMAIN}" ]; then
    echo >&2 "error no MAILGUN_DOMAIN found - please create a credentials.env file"
    exit 1
  fi
  echo "running communications dev server using ${COMMUNICATIONS_IMAGE}"
  echo "using credentials file $DIR/credentials.env"
  docker run ${flags} \
    --name ${DOTMESH_COMMUNICATIONS_NAME} \
    --net dotmesh-dev \
    -e "MAILGUN_API_KEY=$MAILGUN_API_KEY" \
    -e "MAILGUN_DOMAIN=$MAILGUN_DOMAIN" \
    -e "NODE_ENV=development" \
    -e "ACTIVATED=$ACTIVATE_COMMUNICATIONS" \
    -e "BLACKHOLE_DATA_PATH=/tmp" \
    -v "${DIR}/communications:/app" \
    -v "/app/node_modules/" \
    ${COMMUNICATIONS_IMAGE}
}

function communications-stop() {
  echo "stopping communications dev server"
  docker rm -f ${DOTMESH_COMMUNICATIONS_NAME}
}

function router-start() {
  echo "running router using ${ROUTER_IMAGE}"
  docker run -d \
    --name ${DOTMESH_ROUTER_NAME} \
    --net dotmesh-dev \
    -p ${DOTMESH_FRONTEND_PORT}:80 \
    -e NOXY_DEFAULT_HOST=$DOTMESH_FRONTEND_NAME \
    -e NOXY_BILLING_FRONT=/api/v1 \
    -e NOXY_BILLING_HOST=$DOTMESH_BILLING_NAME \
    -e NOXY_RPC_FRONT=/rpc \
    -e NOXY_RPC_HOST=$DOTMESH_SERVER_NAME \
    -e NOXY_RPC_PORT=6969 \
    -e NOXY_STATUS_FRONT=/status \
    -e NOXY_STATUS_HOST=$DOTMESH_SERVER_NAME \
    -e NOXY_STATUS_PORT=6969 \
    $ROUTER_IMAGE
}

function router-stop() {
  echo "stopping router dev server"
  docker rm -f ${DOTMESH_ROUTER_NAME}
}


#
#
# FRONTEND
#
#

function frontend-build() {
  echo "building dotmesh frontend image: ${FRONTEND_IMAGE}"
  bash frontend/rebuild.sh
}


function frontend-volumes() {
  local linkedVolumes=""
  declare -a frontend_volumes=("src" "www" "package.json" "webpack.config.js" "toolbox-variables.js" "yarn.lock")
  # always mount these for local development
  for volume in "${frontend_volumes[@]}"
  do
    linkedVolumes="${linkedVolumes} -v ${DIR}/frontend/${volume}:/app/${volume}"
  done
  # mount modules from templatestack for quick reloading
  # you need to have cloned https://github.com/binocarlos/templatestack.git to the same folder as dotmesh for this to work
  if [ -n "${LINKMODULES}" ]; then
    linkedVolumes="${linkedVolumes} -v ${DIR}/../templatestack/template-tools:/app/node_modules/template-tools"
    linkedVolumes="${linkedVolumes} -v ${DIR}/../templatestack/template-ui:/app/node_modules/template-ui"
  fi
  echo "${linkedVolumes}"
}

function frontend-start() {
  local flags=""
  local linkedVolumes=$(frontend-volumes)
  if [ -n "${CLI}" ]; then
    flags=" --rm -ti --entrypoint sh"
  else
    flags=" -d"
  fi
  USE_FRONTEND_IMAGE="$FRONTEND_BUILDER_IMAGE"
  if [ -n "$PRODUCTION_MODE" ]; then
    USE_FRONTEND_IMAGE="$FRONTEND_IMAGE"
  fi
  echo "running frontend dev server using ${USE_FRONTEND_IMAGE}"
  docker run ${flags} \
    --name ${DOTMESH_FRONTEND_NAME} \
    --net dotmesh-dev \
    -v "${DIR}/frontend:/app" \
    -v "/app/node_modules/" ${linkedVolumes} \
    $USE_FRONTEND_IMAGE
}

function frontend-stop() {
  echo "stopping frontend dev server"
  docker rm -f dotmesh-frontend
}

function frontend-dist() {
  docker run -it --rm \
    -v "${DIR}/frontend:/app" \
    ${FRONTEND_IMAGE} release
}

function frontend-test-build() {
  docker build -t ${NIGHTWATCH_IMAGE} -f ${DIR}/frontend/test/Dockerfile ${DIR}/frontend/test
  mkdir -p ${DIR}/frontend/test/binaries
  cp -f ${DIR}/binaries/Linux/dm ${DIR}/frontend/test/binaries/dm
  docker build -t ${GOTTY_IMAGE} -f ${DIR}/frontend/test/Dockerfile.gotty ${DIR}/frontend/test
}

function frontend-test-prod() {
  frontend-test "${DOTMESH_SERVER_NAME}" "${DOTMESH_SERVER_PORT}"
}

function frontend-test() {
  if [ -n "$TEST_NAME" ]; then
    frontend-test-run
  else
    TEST_NAME="specs/auth.js" frontend-test-run
    TEST_NAME="specs/updatepassword.js" frontend-test-run
    TEST_NAME="specs/branches.js" frontend-test-run
    TEST_NAME="specs/endtoend.js" frontend-test-run
    TEST_NAME="specs/payment.js" frontend-test-run
    TEST_NAME="specs/registeremail.js" frontend-test-run
  fi
}

function frontend-test-run() {
  local apiserver="${DOTMESH_FRONTEND_NAME}"
  local billingserver="${DOTMESH_BILLING_NAME}"
  local timestamp=$(date +%s)
  local TEST_DEBUG_OPT=""
  if [ -n "$TEST_DEBUG" ]; then
    TEST_DEBUG_OPT="--entrypoint sh"
    TEST_NAME=""
  fi
  rm -rf ${DIR}/frontend/.media
  docker run -ti --rm $TEST_DEBUG_OPT \
    --net dotmesh-dev \
    --name ${NIGHTWATCH_NAME} \
    -e "LAUNCH_URL=$DOTMESH_ROUTER_NAME/ui" \
    -e "SELENIUM_HOST=$CHROME_DRIVER_NAME" \
    -e "WAIT_FOR_HOSTS=$DOTMESH_FRONTEND_NAME:80 $DOTMESH_BILLING_NAME:80 $DOTMESH_ROUTER_NAME:80 $CHROME_DRIVER_NAME:4444 $CHROME_DRIVER_NAME:6060 $GOTTY_NAME:$GOTTY_PORT" \
    -e "GOTTY_HOST=$GOTTY_NAME:$GOTTY_PORT" \
    -e "TEST_USER=test${timestamp}" \
    -e "TEST_EMAIL=test${timestamp}@test.com" \
    -e "TEST_PASSWORD=test" \
    -e "QUICK=${QUICK}" \
    -v "${DIR}/frontend/.media/screenshots:/home/node/screenshots" \
    -v "${DIR}/frontend/.media/videos:/home/node/videos" \
    -v "${DIR}/frontend/test/specs:/home/node/specs" \
    -v "${DIR}/frontend/test/lib:/home/node/lib" \
    $NIGHTWATCH_IMAGE $TEST_NAME
}


#
#
# CHROME DRIVER / GOTTY
#
#

function chromedriver-start() {
  docker run -d \
    --net dotmesh-dev \
    --name $CHROME_DRIVER_NAME \
    -e VNC_ENABLED=true \
    -e EXPOSE_X11=true \
    ${CHROME_DRIVER_IMAGE}
}

function chromedriver-start-prod() {
  chromedriver-start "${DOTMESH_SERVER_NAME}"
}

function chromedriver-stop() {
  docker rm -f ${CHROME_DRIVER_NAME} || true
}

function gotty-start() {
  docker run -d \
    --net dotmesh-dev \
    -p $GOTTY_PORT:$GOTTY_PORT \
    --name $GOTTY_NAME \
    -e TERM=linux \
    -v $DIR/binaries/Linux/dm:/usr/local/bin/dm \
    -v /var/run/docker.sock:/var/run/docker.sock \
    -v /var/run/docker.sock:/var/run/docker.sock \
    ${GOTTY_IMAGE}
}

function gotty-stop() {
  docker rm -f $GOTTY_NAME
}


#
#
# UTILS
#
#


function build() {
  NO_PUSH=1 bash rebuild.sh
}

function reset() {
  $DM cluster reset || true
  docker rm -f ${DOTMESH_FRONTEND_NAME} || true
  docker rm -f ${DOTMESH_COMMUNICATIONS_NAME} || true
  docker rm -f ${DOTMESH_BILLING_NAME} || true
  docker rm -f ${CHROME_DRIVER_NAME} || true
  docker rm -f ${GOTTY_NAME} || true
}

function etcdctl() {
  docker exec -ti dotmesh-etcd etcdctl \
    --cert-file=/pki/apiserver.pem \
    --key-file=/pki/apiserver-key.pem \
    --ca-file=/pki/ca.pem \
    --endpoints https://127.0.0.1:42379 "$@"
}

# print the api key for the admin user
function admin-api-key() {
  cat $HOME/.dotmesh/config  | jq -r '.Remotes.local.ApiKey'
}

function vagrant-sync() {
  githash=$(cd $VAGRANT_HOME && git rev-parse HEAD)  
  rm -rf $VAGRANT_GOPATH/.git
  cp -r /vagrant/.git $VAGRANT_GOPATH
  (cd $VAGRANT_GOPATH && git reset --hard $githash)
}

function vagrant-prepare() {
  vagrant-sync
  (cd $VAGRANT_GOPATH && ./prep-tests.sh)
}

function vagrant-test() {
  local SINGLE_TEST=""
  if [ -n "$TEST_NAME" ]; then
    SINGLE_TEST=" -run $TEST_NAME"
  fi
  (cd $VAGRANT_GOPATH && ./test.sh $SINGLE_TEST)
}

function vagrant-gopath() {
  echo $VAGRANT_GOPATH
}

function vagrant-tunnel() {
  local node="$1"
  if [ -z "$node" ]; then
    echo >&2 "usage vagrant-tunnel <containerid>"
    exit 1
  fi
  local ip=$(docker inspect -f '{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}' $node)
  echo http://172.17.1.178:8080
  socat tcp-listen:8080,reuseaddr,fork tcp:$ip:8080
}

eval "$@"
