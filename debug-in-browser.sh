#!/usr/bin/env bash

set -e

if [ "$ADMIN_PW" == "" ]; then
    echo "Please set ADMIN_PW"
    exit 1
fi

if [ "$IFACE" == "" ]; then
    IFACE="eth0"
fi

nodes=`docker ps --format "{{.Names}}" |grep cluster-`

# bash 4 only, associative array for passwords
declare -A passwords

# Get passwords from containers

for node in $nodes; do
    passwords[$node]=`docker exec -ti $node cat /root/.dotmesh/config | jq .Remotes.local.ApiKey`
    passwords[$node]=`echo ${passwords[$node]} |tr -d '"'`
done

declare -A ips

for node in $nodes; do
    ips[$node]=`docker exec -ti $node ifconfig $IFACE | grep "inet addr" | cut -d ':' -f 2 | cut -d ' ' -f 1`
done

echo
echo "If running on a headless VM, try:"
echo "    docker run -d --restart=always --name=tinyproxy --net=host \\"
echo "        dannydirect/tinyproxy:latest ANY"
echo
echo "Then configure your web browser to proxy all HTTP traffic through your VM's IP"
echo "on port 8888."
echo
echo "NOTE: click the 'Use the same proxy server for all protocols' checkbox and delete the text in 'No Proxy for'"
echo
echo "NOTE: you might need to remove the /app/kibana part of the url the very first time to get it to work"
echo
echo "This Chrome plugin makes this change less invasive and easier to change:"
echo "    http://bit.ly/1kL9DhD"
echo
echo "=============================================================================="
echo

for node in $nodes; do
    docker exec -i $node \
        docker run --restart=always -d -v /root:/root \
            --name etcd-browser -p 0.0.0.0:8000:8000 \
            --env ETCD_HOST=${ips[$node]} -e ETCD_PORT=42379 \
            -e ETCDCTL_CA_FILE=/root/.dotmesh/pki/ca.pem \
            -e ETCDCTL_KEY_FILE=/root/.dotmesh/pki/apiserver-key.pem \
            -e ETCDCTL_CERT_FILE=/root/.dotmesh/pki/apiserver.pem \
            -t -i $(hostname).local:80/dotmesh/etcd-browser:v1 nodejs server.js > /dev/null
done

echo "Kibana:                                    http://admin:$ADMIN_PW@localhost:83/"

# node uis
for node in $nodes; do
    echo "$node GUI:  http://admin:${passwords[$node]}@${ips[$node]}:32607/ux"
    echo "$node GUI:  http://admin:${passwords[$node]}@${ips[$node]}:32607/ui"
done

for job in `jobs -p`; do
    wait $job
done

# etcd viewers

for node in $nodes; do
    echo $node etcd: http://${ips[$node]}:8000/
done

# debug
for node in $nodes; do
    docker exec -ti $node \
        docker exec -ti dotmesh-server-inner \
            curl http://localhost:6060/debug/pprof/goroutine?debug=1 > $X.goroutines
done

echo
echo "=============================================================================="
